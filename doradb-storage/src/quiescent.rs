use std::collections::BTreeSet;
use std::fmt;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::ptr::{NonNull, addr_of_mut};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::Duration;
use thiserror::Error;

// Match Arc's soft refcount ceiling while leaving headroom above the panic
// threshold for the fetch-add rollback path.
const MAX_GUARD_COUNT: usize = isize::MAX as usize;
// Keep DAG ids within the same signed-range ceiling used elsewhere instead of
// allowing the global allocator to march toward u64 wraparound.
const MAX_QUI_DAG_ID: u64 = isize::MAX as u64;
const OWNER_DROP_SPIN_LIMIT: u32 = 64;
const OWNER_DROP_YIELD_LIMIT: u32 = 128;
const OWNER_DROP_INITIAL_SLEEP_US: u64 = 50;
const OWNER_DROP_MAX_SLEEP_US: u64 = 1_000;
static NEXT_QUI_DAG_ID: AtomicU64 = AtomicU64::new(1);

struct QuiescentInner<T> {
    guard_count: AtomicUsize,
    value: T,
}

impl<T> QuiescentInner<T> {
    #[inline]
    fn new(value: T) -> Self {
        Self {
            guard_count: AtomicUsize::new(0),
            value,
        }
    }

    #[inline]
    fn acquire_guard(&self) {
        let old_count = self.guard_count.fetch_add(1, Ordering::Relaxed);
        if old_count >= MAX_GUARD_COUNT {
            self.guard_count.fetch_sub(1, Ordering::Relaxed);
            guard_count_overflow();
        }
    }

    #[inline]
    fn release_guard(&self) {
        // Guard release must not touch quiescent metadata after the decrement.
        // Once owner teardown observes zero, it is free to reclaim the entire
        // allocation immediately.
        let old_count = self.guard_count.fetch_sub(1, Ordering::Release);
        if old_count == 0 {
            self.guard_count.fetch_add(1, Ordering::Relaxed);
            guard_count_underflow();
        }
    }

    #[inline]
    fn value_ptr(inner: NonNull<Self>) -> NonNull<T> {
        // SAFETY: `inner` points to a live `QuiescentInner<T>` allocation.
        // Projecting the `value` field with `addr_of_mut!` does not create an
        // intermediate reference and preserves the stable heap address.
        let ptr = unsafe { addr_of_mut!((*inner.as_ptr()).value) };
        NonNull::new(ptr).expect("quiescent inner value pointer")
    }
}

#[cold]
fn guard_count_overflow() -> ! {
    panic!("quiescent guard count overflow");
}

#[cold]
fn guard_count_underflow() -> ! {
    panic!("quiescent guard count underflow");
}

#[cold]
fn qui_handle_count_overflow() -> ! {
    panic!("quiescent handle count overflow");
}

#[cold]
fn qui_handle_count_underflow() -> ! {
    panic!("quiescent handle count underflow");
}

#[cold]
fn qui_dag_id_overflow() -> ! {
    panic!("quiescent dependency graph id overflow");
}

#[cold]
fn qui_handle_leaked(node_id: NodeId, name: &str, handle_count: usize) -> ! {
    panic!(
        "quiescent handle leaked during teardown: node={node_id}, name={name}, outstanding_handles={handle_count}"
    );
}

#[cold]
fn qui_node_dep_not_declared(node_id: NodeId) -> ! {
    panic!(
        "QuiNodeDeps::dep requires the handle to be declared first with QuiNodeBuilder::depends_on: undeclared dependency node={node_id}"
    );
}

/// Owns a heap-allocated value that can be shared by quiescent guards.
///
/// The owner allocation is pinned for the full lifetime of the box, so the
/// stored value stays at a stable heap address while guards exist. Dropping the
/// owner blocks until all outstanding guards have been released. Teardown is a
/// cold polling path with bounded spin/yield and capped sleep backoff so guard
/// release stays on a single-atomic hot path. Callers must therefore avoid
/// dropping the owner while still holding guards themselves, or teardown will
/// block forever.
pub struct QuiescentBox<T> {
    inner: Pin<Box<QuiescentInner<T>>>,
}

impl<T> QuiescentBox<T> {
    /// Creates a new quiescent owner around `value`.
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: Box::pin(QuiescentInner::new(value)),
        }
    }

    #[inline]
    fn inner_ptr(&self) -> NonNull<QuiescentInner<T>> {
        NonNull::from(self.inner.as_ref().get_ref())
    }

    /// Creates a shared keepalive guard to the owned value.
    ///
    /// Guard creation is intentionally cheap: it increments one keepalive
    /// counter and stores raw pointers back to the owner allocation.
    #[inline]
    pub fn guard(&self) -> QuiescentGuard<T> {
        QuiescentGuard::new(self.inner_ptr())
    }

    /// Creates a long-lived dependency edge to the owned value.
    ///
    /// Unlike transient [`QuiescentGuard`] borrows, a [`QuiDep`] is intended to
    /// be stored inside dependent components or worker closures so owner
    /// teardown waits until the dependency edge is released.
    #[inline]
    pub fn dep(&self) -> QuiDep<T> {
        QuiDep::new(self.guard())
    }
}

impl<T> Deref for QuiescentBox<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner.as_ref().get_ref().value
    }
}

impl<T> Drop for QuiescentBox<T> {
    #[inline]
    fn drop(&mut self) {
        let inner = self.inner.as_ref().get_ref();
        let mut attempts = 0u32;
        // Owner teardown is cold, so use backoff here and keep guard release as
        // a single atomic decrement on the hot path.
        while inner.guard_count.load(Ordering::Acquire) != 0 {
            if attempts < OWNER_DROP_SPIN_LIMIT {
                std::hint::spin_loop();
            } else if attempts < OWNER_DROP_YIELD_LIMIT {
                thread::yield_now();
            } else {
                let sleep_shift = (attempts - OWNER_DROP_YIELD_LIMIT).min(5);
                let sleep_us =
                    (OWNER_DROP_INITIAL_SLEEP_US << sleep_shift).min(OWNER_DROP_MAX_SLEEP_US);
                thread::sleep(Duration::from_micros(sleep_us));
            }
            attempts = attempts.saturating_add(1);
        }
    }
}

/// Cloneable shared access handle for a [`QuiescentBox`]-owned value.
///
/// Each guard keeps the owner allocation alive until the guard is dropped.
/// Guards only provide shared access and dereference to `&T`.
pub struct QuiescentGuard<T> {
    ptr: NonNull<T>,
    inner: NonNull<QuiescentInner<T>>,
}

impl<T> QuiescentGuard<T> {
    #[inline]
    fn new(inner: NonNull<QuiescentInner<T>>) -> Self {
        // SAFETY: `inner` originates from a live `QuiescentBox` allocation and
        // remains valid while the acquired keepalive count is held by the guard.
        let inner_ref = unsafe { inner.as_ref() };
        inner_ref.acquire_guard();
        Self {
            ptr: QuiescentInner::value_ptr(inner),
            inner,
        }
    }

    #[inline]
    fn inner_ref(&self) -> &QuiescentInner<T> {
        // SAFETY: guards increment the keepalive count on creation and release
        // it only in `Drop`, so the owner allocation remains live here.
        unsafe { self.inner.as_ref() }
    }

    /// Returns the raw pointer to the guarded value.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr() as *const T
    }
}

impl<T> Clone for QuiescentGuard<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self::new(self.inner)
    }
}

impl<T> Deref for QuiescentGuard<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY: the guard holds one keepalive count for the owner
        // allocation, so the pointee stays valid for the full guard lifetime.
        unsafe { self.ptr.as_ref() }
    }
}

impl<T> Drop for QuiescentGuard<T> {
    #[inline]
    fn drop(&mut self) {
        self.inner_ref().release_guard();
    }
}

// SAFETY: moving a guard to another thread only exposes shared `&T` access,
// which is thread-safe exactly when `T: Sync`.
unsafe impl<T: Sync> Send for QuiescentGuard<T> {}

// SAFETY: sharing references to guards is equivalent to sharing references to
// `&T`, so this is sound exactly when `T: Sync`.
unsafe impl<T: Sync> Sync for QuiescentGuard<T> {}

#[derive(Debug)]
struct QuiHandleState {
    handle_count: AtomicUsize,
}

impl QuiHandleState {
    #[inline]
    fn new() -> Self {
        Self {
            handle_count: AtomicUsize::new(1),
        }
    }

    #[inline]
    fn acquire_handle(&self) {
        let old_count = self.handle_count.fetch_add(1, Ordering::Relaxed);
        if old_count >= MAX_GUARD_COUNT {
            self.handle_count.fetch_sub(1, Ordering::Relaxed);
            qui_handle_count_overflow();
        }
    }

    #[inline]
    fn release_handle(&self) {
        let old_count = self.handle_count.fetch_sub(1, Ordering::Release);
        if old_count == 0 {
            self.handle_count.fetch_add(1, Ordering::Relaxed);
            qui_handle_count_underflow();
        }
    }

    #[inline]
    fn assert_no_handles(&self, node_id: NodeId, name: &str) {
        let handle_count = self.handle_count.load(Ordering::Acquire);
        if handle_count != 0 {
            qui_handle_leaked(node_id, name, handle_count);
        }
    }
}

/// Long-lived dependency edge to a quiescent-owned value.
///
/// `QuiDep<T>` is a thin wrapper around [`QuiescentGuard<T>`] for component
/// fields and worker-thread captures that must keep a dependency alive until
/// the dependent shuts down.
///
/// For DAG-managed components, persistent `QuiDep` values must come from
/// [`QuiNodeDeps::dep`] inside [`QuiNodeBuilder::build`] or
/// [`QuiNodeBuilder::build_async`]. That keeps runtime keepalive edges aligned
/// with the teardown order enforced by [`QuiDAG::drop`].
pub struct QuiDep<T> {
    guard: QuiescentGuard<T>,
}

impl<T> QuiDep<T> {
    #[inline]
    fn new(guard: QuiescentGuard<T>) -> Self {
        Self { guard }
    }

    /// Clones a transient shared guard from this dependency edge.
    #[inline]
    pub fn guard(&self) -> QuiescentGuard<T> {
        self.guard.clone()
    }

    /// Returns the raw pointer to the dependency target.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.guard.as_ptr()
    }
}

impl<T> Clone for QuiDep<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            guard: self.guard.clone(),
        }
    }
}

impl<T> Deref for QuiDep<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

/// Opaque identifier of one node registered in a [`QuiDAG`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId {
    dag_id: u64,
    slot: usize,
}

impl NodeId {
    #[inline]
    const fn new(dag_id: u64, slot: usize) -> Self {
        Self { dag_id, slot }
    }
}

impl fmt::Display for NodeId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.dag_id, self.slot)
    }
}

/// Error returned by [`QuiDAG`] registration or validation.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QuiDagError {
    /// Structural edits were attempted after the graph was sealed.
    #[error("quiescent dependency graph is sealed")]
    Sealed,
    /// One referenced node id does not belong to this graph.
    #[error("unknown quiescent dependency graph node {node}")]
    UnknownNode { node: NodeId },
    /// The graph contains a cycle and therefore has no valid drop order.
    #[error("quiescent dependency graph contains a cycle involving nodes {nodes:?}")]
    Cycle { nodes: Box<[NodeId]> },
}

/// Typed access handle for a component registered in a [`QuiDAG`].
///
/// Handles are non-owning and can be cloned freely while the graph is live.
/// They are intended for graph construction and transient access, not for
/// teardown. Callers must drop every `QuiHandle` clone before the graph starts
/// dropping owners. Violating that contract is a bug and will panic during
/// `QuiDAG` teardown.
///
/// Persistent runtime dependencies are intentionally not available from this
/// type. Declare the edge with [`QuiNodeBuilder::depends_on`] and create the
/// stored [`QuiDep`] through [`QuiNodeDeps::dep`] inside
/// [`QuiNodeBuilder::build`] or [`QuiNodeBuilder::build_async`]. That keeps
/// long-lived `QuiDep` values synchronized with the drop ordering used by
/// [`QuiDAG::drop`].
pub struct QuiHandle<T> {
    id: NodeId,
    owner: Weak<QuiescentBox<T>>,
    handle_state: Arc<QuiHandleState>,
}

impl<T> QuiHandle<T> {
    #[inline]
    fn new(id: NodeId, owner: &Arc<QuiescentBox<T>>, handle_state: &Arc<QuiHandleState>) -> Self {
        Self {
            id,
            owner: Arc::downgrade(owner),
            handle_state: Arc::clone(handle_state),
        }
    }

    /// Returns this node's graph identifier.
    #[inline]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// Attempts to create a transient shared guard to the registered value.
    ///
    /// This returns `None` after the graph has already dropped the owner.
    /// Callers must not race this with graph teardown; keeping any handle alive
    /// during `QuiDAG` drop violates the handle contract and will panic.
    ///
    /// Use this for short-lived access only. If a component needs to retain a
    /// dependency after construction, declare it through
    /// [`QuiNodeBuilder::depends_on`] and obtain the persistent [`QuiDep`] from
    /// [`QuiNodeDeps::dep`] instead of storing a guard derived from a handle.
    #[inline]
    pub fn try_guard(&self) -> Option<QuiescentGuard<T>> {
        self.owner.upgrade().map(|owner| owner.guard())
    }

    /// Creates a transient shared guard to the registered value.
    ///
    /// # Panics
    ///
    /// Panics if the graph has already dropped this node's owner.
    #[inline]
    pub fn guard(&self) -> QuiescentGuard<T> {
        match self.try_guard() {
            Some(guard) => guard,
            None => qui_handle_owner_dropped(),
        }
    }
}

impl<T> Clone for QuiHandle<T> {
    #[inline]
    fn clone(&self) -> Self {
        self.handle_state.acquire_handle();
        Self {
            id: self.id,
            owner: self.owner.clone(),
            handle_state: Arc::clone(&self.handle_state),
        }
    }
}

impl<T> Drop for QuiHandle<T> {
    #[inline]
    fn drop(&mut self) {
        self.handle_state.release_handle();
    }
}

#[cold]
fn qui_handle_owner_dropped() -> ! {
    panic!("quiescent handle used after owner drop");
}

/// Builder-side capability for creating long-lived dependencies.
///
/// `QuiNodeDeps` is handed to a [`QuiNodeBuilder::build`] or
/// [`QuiNodeBuilder::build_async`] closure after the builder has recorded which
/// nodes the new component depends on. Only handles declared with
/// [`QuiNodeBuilder::depends_on`] can be turned into a persistent [`QuiDep`].
///
/// This keeps two things in sync:
/// 1. runtime keepalive edges held in fields and worker closures via
///    [`QuiDep`], and
/// 2. teardown order enforced by [`QuiDAG::drop`].
pub struct QuiNodeDeps {
    declared: Box<[NodeId]>,
}

impl QuiNodeDeps {
    #[inline]
    fn new(declared: &[NodeId]) -> Self {
        let declared = declared
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { declared }
    }

    #[inline]
    fn contains(&self, node: NodeId) -> bool {
        self.declared.binary_search(&node).is_ok()
    }

    /// Creates a persistent dependency edge for one previously declared node.
    ///
    /// A typical constructor flow is:
    /// 1. `dag.node("a")?.depends_on(&b)?`
    /// 2. `build(|deps| { let dep_b = deps.dep(&b); ... })`
    ///
    /// That lets the new component store `dep_b`, clone it into worker
    /// threads, and still guarantees that `QuiDAG::drop` will tear down the
    /// dependent before the dependency.
    ///
    /// # Panics
    ///
    /// Panics if `handle` was not declared with [`QuiNodeBuilder::depends_on`]
    /// for this builder, or if the owner was already dropped.
    #[inline]
    pub fn dep<T>(&self, handle: &QuiHandle<T>) -> QuiDep<T> {
        if !self.contains(handle.id()) {
            qui_node_dep_not_declared(handle.id());
        }
        QuiDep::new(handle.guard())
    }
}

/// Builder for one DAG-managed component that may need persistent dependencies.
///
/// Unlike `insert_with_deps`, this builder keeps dependency declarations in
/// local builder state until the final value has been constructed successfully.
/// Dropping an unused builder leaves the graph unchanged, which makes the API
/// robust for fallible or async construction without introducing half-built
/// graph nodes.
///
/// Typical usage:
/// - call [`QuiDAG::node`] to start the builder,
/// - declare runtime dependencies with [`Self::depends_on`],
/// - declare pure teardown ordering with [`Self::drop_before`],
/// - then finish with [`Self::build`] or [`Self::build_async`].
///
/// Inside the build closure, use [`QuiNodeDeps::dep`] to mint the persistent
/// [`QuiDep`] values that will be stored in the new component or cloned into
/// worker threads.
#[must_use = "call build/build_async to insert the node, or drop the builder to leave the graph unchanged"]
pub struct QuiNodeBuilder<'a> {
    dag: &'a mut QuiDAG,
    name: String,
    runtime_deps: Vec<NodeId>,
    order_only_edges: Vec<NodeId>,
}

impl<'a> QuiNodeBuilder<'a> {
    #[inline]
    fn new(dag: &'a mut QuiDAG, name: String) -> Self {
        Self {
            dag,
            name,
            runtime_deps: Vec::new(),
            order_only_edges: Vec::new(),
        }
    }

    /// Declares that the new node has a runtime dependency on `handle`.
    ///
    /// Only nodes declared here may be turned into persistent [`QuiDep`] values
    /// through [`QuiNodeDeps::dep`]. That is what keeps runtime keepalive edges
    /// aligned with `QuiDAG` drop ordering.
    pub fn depends_on<T>(
        mut self,
        handle: &QuiHandle<T>,
    ) -> std::result::Result<Self, QuiDagError> {
        self.dag.validate_node(handle.id())?;
        push_unique_node(&mut self.runtime_deps, handle.id());
        Ok(self)
    }

    /// Declares a teardown-only ordering edge for the new node.
    ///
    /// This affects [`QuiDAG::drop`] order but does not authorize
    /// [`QuiNodeDeps::dep`] to create a persistent dependency.
    pub fn drop_before<T>(
        mut self,
        handle: &QuiHandle<T>,
    ) -> std::result::Result<Self, QuiDagError> {
        self.dag.validate_node(handle.id())?;
        push_unique_node(&mut self.order_only_edges, handle.id());
        Ok(self)
    }

    /// Builds the value synchronously and inserts it into the graph on success.
    ///
    /// The graph is not mutated until `build` has returned `Ok(value)` from the
    /// closure. If construction fails, the builder is dropped and the graph
    /// remains unchanged.
    pub fn build<T: 'static, E, F>(self, build: F) -> std::result::Result<QuiHandle<T>, E>
    where
        F: FnOnce(QuiNodeDeps) -> std::result::Result<T, E>,
        E: From<QuiDagError>,
    {
        let Self {
            dag,
            name,
            runtime_deps,
            order_only_edges,
        } = self;
        let deps = QuiNodeDeps::new(&runtime_deps);
        let value = build(deps)?;
        dag.insert_with_deps(
            name,
            value,
            merge_declared_edges(runtime_deps, order_only_edges),
        )
        .map_err(E::from)
    }

    /// Async variant of [`Self::build`].
    ///
    /// This keeps `&mut QuiDAG` borrowed across the await, which intentionally
    /// serializes graph assembly in exchange for the simpler invariant that the
    /// graph stays unchanged until construction succeeds.
    pub async fn build_async<T: 'static, E, F, Fut>(
        self,
        build: F,
    ) -> std::result::Result<QuiHandle<T>, E>
    where
        F: FnOnce(QuiNodeDeps) -> Fut,
        Fut: Future<Output = std::result::Result<T, E>>,
        E: From<QuiDagError>,
    {
        let Self {
            dag,
            name,
            runtime_deps,
            order_only_edges,
        } = self;
        let deps = QuiNodeDeps::new(&runtime_deps);
        let value = build(deps).await?;
        dag.insert_with_deps(
            name,
            value,
            merge_declared_edges(runtime_deps, order_only_edges),
        )
        .map_err(E::from)
    }
}

#[inline]
fn push_unique_node(nodes: &mut Vec<NodeId>, node: NodeId) {
    if !nodes.contains(&node) {
        nodes.push(node);
    }
}

#[inline]
fn merge_declared_edges(runtime_deps: Vec<NodeId>, order_only_edges: Vec<NodeId>) -> Vec<NodeId> {
    runtime_deps
        .into_iter()
        .chain(order_only_edges)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

trait ErasedQuiOwner {
    fn drop_owner(&mut self);
}

struct TypedQuiOwner<T> {
    node_id: NodeId,
    name: String,
    owner: Option<Arc<QuiescentBox<T>>>,
    handle_state: Arc<QuiHandleState>,
}

impl<T> ErasedQuiOwner for TypedQuiOwner<T> {
    #[inline]
    fn drop_owner(&mut self) {
        self.handle_state
            .assert_no_handles(self.node_id, &self.name);
        drop(self.owner.take());
    }
}

struct QuiDagNode {
    _name: String,
    edges: Vec<usize>,
    owner: Box<dyn ErasedQuiOwner>,
}

/// Dependency-aware owner that drops registered components in graph order.
///
/// Each edge represents one teardown ordering constraint: if `A` depends on
/// `B`, then `A` must be dropped before `B`. Callers must register all
/// components, add any teardown-only ordering edges, and then call
/// [`Self::seal`] to validate the graph and freeze its structure. All
/// [`QuiHandle`] values must be dropped before this graph starts teardown; the
/// graph asserts that contract and panics on leaked handles.
///
/// Use [`Self::insert`] or [`Self::insert_with_deps`] when the final value is
/// already constructed. Use [`Self::node`] when construction is fallible,
/// asynchronous, or needs to store persistent [`QuiDep`] values. The builder
/// records dependency declarations locally and only mutates the graph after the
/// constructor returns `Ok(value)`.
pub struct QuiDAG {
    dag_id: u64,
    nodes: Vec<QuiDagNode>,
    drop_order: Option<Vec<usize>>,
}

impl Default for QuiDAG {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl QuiDAG {
    /// Creates an empty quiescent dependency graph.
    #[inline]
    pub fn new() -> Self {
        Self {
            dag_id: next_qui_dag_id(),
            nodes: Vec::new(),
            drop_order: None,
        }
    }

    /// Starts building one node without mutating the graph yet.
    ///
    /// The returned builder stores dependency declarations locally. If the
    /// builder is dropped before `build` or `build_async` completes
    /// successfully, this graph is unchanged.
    #[inline]
    pub fn node(
        &mut self,
        name: impl Into<String>,
    ) -> std::result::Result<QuiNodeBuilder<'_>, QuiDagError> {
        self.ensure_mutable()?;
        Ok(QuiNodeBuilder::new(self, name.into()))
    }

    /// Registers one component with no initial dependency edges.
    #[inline]
    pub fn insert<T: 'static>(
        &mut self,
        name: impl Into<String>,
        value: T,
    ) -> std::result::Result<QuiHandle<T>, QuiDagError> {
        self.insert_with_deps(name, value, std::iter::empty())
    }

    /// Registers one already-constructed component together with its
    /// dependency edges.
    ///
    /// Each `depends_on` node must already be registered in the graph. If the
    /// component constructor needs to store persistent [`QuiDep`] values, use
    /// [`Self::node`] instead so edge declaration and `QuiDep` creation stay in
    /// sync.
    pub fn insert_with_deps<T: 'static, I>(
        &mut self,
        name: impl Into<String>,
        value: T,
        depends_on: I,
    ) -> std::result::Result<QuiHandle<T>, QuiDagError>
    where
        I: IntoIterator<Item = NodeId>,
    {
        self.ensure_mutable()?;
        let edges = self.collect_edges(depends_on)?;
        let id = NodeId::new(self.dag_id, self.nodes.len());
        let owner = Arc::new(QuiescentBox::new(value));
        let handle_state = Arc::new(QuiHandleState::new());
        let handle = QuiHandle::new(id, &owner, &handle_state);
        let name = name.into();
        self.nodes.push(QuiDagNode {
            _name: name.clone(),
            edges,
            owner: Box::new(TypedQuiOwner {
                node_id: id,
                name,
                owner: Some(owner),
                handle_state,
            }),
        });
        Ok(handle)
    }

    /// Adds a teardown-only ordering edge.
    ///
    /// After sealing, the graph will always drop `before` ahead of `after`
    /// even if the two components do not have a normal runtime dependency.
    pub fn drop_before(
        &mut self,
        before: NodeId,
        after: NodeId,
    ) -> std::result::Result<(), QuiDagError> {
        self.ensure_mutable()?;
        let before_idx = self.validate_node(before)?;
        let after_idx = self.validate_node(after)?;
        self.nodes[before_idx].edges.push(after_idx);
        Ok(())
    }

    /// Validates the graph, computes one deterministic drop order, and freezes
    /// the structure against further mutation.
    pub fn seal(&mut self) -> std::result::Result<(), QuiDagError> {
        self.ensure_mutable()?;
        self.drop_order = Some(self.compute_drop_order()?);
        Ok(())
    }

    #[inline]
    fn ensure_mutable(&self) -> std::result::Result<(), QuiDagError> {
        if self.drop_order.is_some() {
            Err(QuiDagError::Sealed)
        } else {
            Ok(())
        }
    }

    fn collect_edges<I>(&self, edges: I) -> std::result::Result<Vec<usize>, QuiDagError>
    where
        I: IntoIterator<Item = NodeId>,
    {
        let mut collected = Vec::new();
        for edge in edges {
            collected.push(self.validate_node(edge)?);
        }
        Ok(collected)
    }

    #[inline]
    fn validate_node(&self, node: NodeId) -> std::result::Result<usize, QuiDagError> {
        if node.dag_id == self.dag_id && node.slot < self.nodes.len() {
            Ok(node.slot)
        } else {
            Err(QuiDagError::UnknownNode { node })
        }
    }

    fn compute_drop_order(&self) -> std::result::Result<Vec<usize>, QuiDagError> {
        let mut indegree = vec![0usize; self.nodes.len()];
        let mut outgoing = vec![Vec::new(); self.nodes.len()];
        for (idx, node) in self.nodes.iter().enumerate() {
            let unique_edges = node.edges.iter().copied().collect::<BTreeSet<_>>();
            outgoing[idx] = unique_edges.iter().copied().collect();
            for dep in unique_edges {
                indegree[dep] += 1;
            }
        }

        let mut ready = indegree
            .iter()
            .enumerate()
            .filter_map(|(idx, degree)| (*degree == 0).then_some(idx))
            .collect::<BTreeSet<_>>();
        let mut order = Vec::with_capacity(self.nodes.len());
        while let Some(next_idx) = ready.pop_first() {
            order.push(next_idx);
            for dep in &outgoing[next_idx] {
                indegree[*dep] -= 1;
                if indegree[*dep] == 0 {
                    ready.insert(*dep);
                }
            }
        }

        if order.len() != self.nodes.len() {
            let nodes = indegree
                .iter()
                .enumerate()
                .filter_map(|(idx, degree)| (*degree != 0).then_some(NodeId::new(self.dag_id, idx)))
                .collect::<Vec<_>>()
                .into_boxed_slice();
            return Err(QuiDagError::Cycle { nodes });
        }
        Ok(order)
    }
}

impl Drop for QuiDAG {
    #[inline]
    fn drop(&mut self) {
        if let Some(drop_order) = self.drop_order.take() {
            for node_idx in drop_order {
                self.nodes[node_idx].owner.drop_owner();
            }
        } else {
            for node in self.nodes.iter_mut().rev() {
                node.owner.drop_owner();
            }
        }
    }
}

#[inline]
fn next_qui_dag_id() -> u64 {
    next_qui_dag_id_from(&NEXT_QUI_DAG_ID)
}

#[inline]
fn next_qui_dag_id_from(counter: &AtomicU64) -> u64 {
    let dag_id = counter.fetch_add(1, Ordering::Relaxed);
    if dag_id >= MAX_QUI_DAG_ID {
        counter.fetch_sub(1, Ordering::Relaxed);
        qui_dag_id_overflow();
    }
    dag_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use std::{panic, panic::AssertUnwindSafe};

    struct DropSpy {
        dropped: Arc<AtomicBool>,
    }

    impl Drop for DropSpy {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    struct DropOrderSpy {
        name: &'static str,
        drops: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Drop for DropOrderSpy {
        fn drop(&mut self) {
            self.drops.lock().unwrap().push(self.name);
        }
    }

    struct WorkerGroup {
        workers: Vec<thread::JoinHandle<()>>,
    }

    impl Drop for WorkerGroup {
        fn drop(&mut self) {
            for worker in self.workers.drain(..) {
                worker.join().unwrap();
            }
        }
    }

    fn panic_message(payload: Box<dyn Any + Send>) -> String {
        match payload.downcast::<String>() {
            Ok(msg) => *msg,
            Err(payload) => match payload.downcast::<&'static str>() {
                Ok(msg) => (*msg).to_string(),
                Err(_) => "<non-string panic payload>".to_string(),
            },
        }
    }

    #[test]
    fn test_next_qui_dag_id_from_allocates_monotonic_ids() {
        let counter = AtomicU64::new(1);

        assert_eq!(next_qui_dag_id_from(&counter), 1);
        assert_eq!(next_qui_dag_id_from(&counter), 2);
        assert_eq!(next_qui_dag_id_from(&counter), 3);
        assert_eq!(counter.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn test_next_qui_dag_id_from_panics_at_isize_max_boundary() {
        let counter = AtomicU64::new(MAX_QUI_DAG_ID - 1);

        assert_eq!(next_qui_dag_id_from(&counter), MAX_QUI_DAG_ID - 1);
        let res = panic::catch_unwind(AssertUnwindSafe(|| next_qui_dag_id_from(&counter)));
        assert!(res.is_err());
        assert_eq!(counter.load(Ordering::Relaxed), MAX_QUI_DAG_ID);
    }

    #[test]
    fn test_next_qui_dag_id_from_overflow_rollback_preserves_counter() {
        let counter = AtomicU64::new(MAX_QUI_DAG_ID);

        let res = panic::catch_unwind(AssertUnwindSafe(|| next_qui_dag_id_from(&counter)));
        assert!(res.is_err());
        assert_eq!(counter.load(Ordering::Relaxed), MAX_QUI_DAG_ID);
    }

    #[test]
    fn test_quiescent_guard_deref_and_as_ptr() {
        let owner = QuiescentBox::new(String::from("hello"));
        let guard = owner.guard();
        let owner_ptr = std::ptr::from_ref::<String>(&owner);
        assert_eq!(&*owner, "hello");
        assert_eq!(&*guard, "hello");
        assert_eq!(guard.as_ptr(), owner_ptr);
    }

    #[test]
    fn test_quiescent_guard_clone_keeps_same_pointer() {
        let owner = QuiescentBox::new(vec![1u64, 2, 3, 4]);
        let guard = owner.guard();
        let guard_clone = guard.clone();
        let owner_ptr = std::ptr::from_ref::<Vec<u64>>(&owner);
        assert_eq!(guard.as_ptr(), owner_ptr);
        assert_eq!(guard_clone.as_ptr(), owner_ptr);
        assert_eq!(guard.iter().sum::<u64>(), 10);
        assert_eq!(guard_clone.iter().sum::<u64>(), 10);
    }

    #[test]
    fn test_quiescent_guard_is_send_for_sync_types() {
        let owner = QuiescentBox::new(vec![1u64, 2, 3, 4]);
        let owner_ptr = std::ptr::from_ref::<Vec<u64>>(&owner) as usize;
        let mut handles = Vec::new();
        for _ in 0..4 {
            let guard = owner.guard();
            handles.push(thread::spawn(move || {
                assert_eq!(guard.iter().sum::<u64>(), 10);
                guard.as_ptr() as usize
            }));
        }
        for handle in handles {
            assert_eq!(handle.join().unwrap(), owner_ptr);
        }
    }

    #[test]
    fn test_quiescent_guard_overflow_panics_without_mutating_count() {
        let owner = QuiescentBox::new(7u64);
        let inner = owner.inner.as_ref().get_ref();
        inner.guard_count.store(MAX_GUARD_COUNT, Ordering::Relaxed);

        let res = panic::catch_unwind(AssertUnwindSafe(|| owner.guard()));
        assert!(res.is_err());
        assert_eq!(inner.guard_count.load(Ordering::Relaxed), MAX_GUARD_COUNT);

        inner.guard_count.store(0, Ordering::Relaxed);
    }

    #[test]
    fn test_quiescent_box_drop_waits_for_last_guard() {
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = QuiescentBox::new(DropSpy {
            dropped: Arc::clone(&dropped),
        });
        let guard = owner.guard();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            drop(owner);
            done_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(!dropped.load(Ordering::Acquire));
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

        drop(guard);

        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(dropped.load(Ordering::Acquire));
        handle.join().unwrap();
    }

    #[test]
    fn test_quiescent_box_drop_waits_for_all_guard_clones() {
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = QuiescentBox::new(DropSpy {
            dropped: Arc::clone(&dropped),
        });
        let guard = owner.guard();
        let guard_clone = guard.clone();
        let (release_tx, release_rx) = mpsc::channel();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let clone_handle = thread::spawn(move || {
            release_rx.recv().unwrap();
            drop(guard_clone);
        });
        let owner_handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            drop(owner);
            done_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(guard);
        assert!(!dropped.load(Ordering::Acquire));
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());

        release_tx.send(()).unwrap();

        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(dropped.load(Ordering::Acquire));
        clone_handle.join().unwrap();
        owner_handle.join().unwrap();
    }

    #[test]
    fn test_quiescent_box_dep_keeps_owner_alive() {
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = QuiescentBox::new(DropSpy {
            dropped: Arc::clone(&dropped),
        });
        let dep = owner.dep();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            drop(owner);
            done_tx.send(()).unwrap();
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(done_rx.recv_timeout(Duration::from_millis(100)).is_err());
        assert!(!dropped.load(Ordering::Acquire));

        drop(dep);

        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(dropped.load(Ordering::Acquire));
        handle.join().unwrap();
    }

    #[test]
    fn test_quidag_linear_dependency_drop_order() {
        let drops = Arc::new(Mutex::new(Vec::new()));
        let mut dag = QuiDAG::new();
        let c = dag
            .insert(
                "c",
                DropOrderSpy {
                    name: "c",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();
        let b = dag
            .insert_with_deps(
                "b",
                DropOrderSpy {
                    name: "b",
                    drops: Arc::clone(&drops),
                },
                [c.id()],
            )
            .unwrap();
        dag.insert_with_deps(
            "a",
            DropOrderSpy {
                name: "a",
                drops: Arc::clone(&drops),
            },
            [b.id()],
        )
        .unwrap();
        dag.seal().unwrap();
        drop(b);
        drop(c);

        drop(dag);

        assert_eq!(drops.lock().unwrap().as_slice(), &["a", "b", "c"]);
    }

    #[test]
    fn test_quidag_shared_dependency_drop_order() {
        let drops = Arc::new(Mutex::new(Vec::new()));
        let mut dag = QuiDAG::new();
        let shared = dag
            .insert(
                "shared",
                DropOrderSpy {
                    name: "shared",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();
        dag.insert_with_deps(
            "left",
            DropOrderSpy {
                name: "left",
                drops: Arc::clone(&drops),
            },
            [shared.id()],
        )
        .unwrap();
        dag.insert_with_deps(
            "right",
            DropOrderSpy {
                name: "right",
                drops: Arc::clone(&drops),
            },
            [shared.id()],
        )
        .unwrap();
        dag.seal().unwrap();
        drop(shared);

        drop(dag);

        assert_eq!(
            drops.lock().unwrap().as_slice(),
            &["left", "right", "shared"]
        );
    }

    #[test]
    fn test_quidag_cycle_rejected_on_seal() {
        let mut dag = QuiDAG::new();
        let a = dag.insert("a", ()).unwrap();
        let b = dag.insert_with_deps("b", (), [a.id()]).unwrap();
        dag.drop_before(a.id(), b.id()).unwrap();

        assert!(matches!(dag.seal(), Err(QuiDagError::Cycle { .. })));
        drop(b);
        drop(a);
    }

    #[test]
    fn test_quidag_drop_before_adds_teardown_only_edge() {
        let drops = Arc::new(Mutex::new(Vec::new()));
        let mut dag = QuiDAG::new();
        let table_fs = dag
            .insert(
                "table_fs",
                DropOrderSpy {
                    name: "table_fs",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();
        let disk_pool = dag
            .insert(
                "disk_pool",
                DropOrderSpy {
                    name: "disk_pool",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();
        dag.drop_before(table_fs.id(), disk_pool.id()).unwrap();
        dag.seal().unwrap();
        drop(disk_pool);
        drop(table_fs);

        drop(dag);

        assert_eq!(drops.lock().unwrap().as_slice(), &["table_fs", "disk_pool"]);
    }

    #[test]
    fn test_quidag_rejects_foreign_node_ids() {
        let mut dag1 = QuiDAG::new();
        let a = dag1.insert("a", ()).unwrap();
        let mut dag2 = QuiDAG::new();
        let b = dag2.insert("b", ()).unwrap();

        assert!(matches!(
            dag1.insert_with_deps("dep", (), [b.id()]),
            Err(QuiDagError::UnknownNode { node }) if node == b.id()
        ));
        assert!(matches!(
            dag1.drop_before(a.id(), b.id()),
            Err(QuiDagError::UnknownNode { node }) if node == b.id()
        ));
        assert!(matches!(
            dag1.node("builder_dep").unwrap().depends_on(&b),
            Err(QuiDagError::UnknownNode { node }) if node == b.id()
        ));
        assert!(matches!(
            dag1.node("builder_order").unwrap().drop_before(&b),
            Err(QuiDagError::UnknownNode { node }) if node == b.id()
        ));
        drop(b);
        drop(a);
    }

    #[test]
    fn test_quidag_error_converts_to_storage_error() {
        let err: crate::error::Error = QuiDagError::Cycle {
            nodes: vec![NodeId::new(7, 11)].into_boxed_slice(),
        }
        .into();
        assert!(matches!(
            err,
            crate::error::Error::QuiescentDag(QuiDagError::Cycle { .. })
        ));
    }

    #[test]
    fn test_quidag_builder_rejects_undeclared_dep() {
        let mut dag = QuiDAG::new();
        let b = dag.insert("b", ()).unwrap();
        let c = dag.insert("c", ()).unwrap();

        let msg = panic_message(
            panic::catch_unwind(AssertUnwindSafe(|| {
                let _: std::result::Result<QuiHandle<()>, crate::error::Error> = dag
                    .node("a")
                    .unwrap()
                    .depends_on(&b)
                    .unwrap()
                    .build(|deps| {
                        let _ = deps.dep(&c);
                        Ok(())
                    });
            }))
            .unwrap_err(),
        );
        assert!(msg.contains("QuiNodeBuilder::depends_on"));

        drop(c);
        drop(b);
        dag.seal().unwrap();
        drop(dag);
    }

    #[test]
    fn test_quidag_builder_failure_leaves_graph_unchanged() {
        let drops = Arc::new(Mutex::new(Vec::new()));
        let mut dag = QuiDAG::new();
        let dep = dag
            .insert(
                "dep",
                DropOrderSpy {
                    name: "dep",
                    drops: Arc::clone(&drops),
                },
            )
            .unwrap();

        let err = dag
            .node("failed")
            .unwrap()
            .depends_on(&dep)
            .unwrap()
            .build::<(), crate::error::Error, _>(|deps| {
                let dep_guard = deps.dep(&dep);
                assert_eq!(dep_guard.name, "dep");
                Err(crate::error::Error::InvalidState)
            });
        assert!(matches!(err, Err(crate::error::Error::InvalidState)));

        dag.seal().unwrap();
        drop(dep);
        drop(dag);

        assert_eq!(drops.lock().unwrap().as_slice(), &["dep"]);
    }

    #[test]
    fn test_quidag_build_async_inserts_node_after_success() {
        let mut dag = QuiDAG::new();
        let dep = dag.insert("dep", 41usize).unwrap();
        let dep_for_build = dep.clone();

        let node = block_on(
            dag.node("async")
                .unwrap()
                .depends_on(&dep)
                .unwrap()
                .build_async::<usize, crate::error::Error, _, _>(move |deps| async move {
                    let dep = deps.dep(&dep_for_build);
                    Ok(*dep + 1)
                }),
        )
        .unwrap();

        assert_eq!(*node.guard(), 42);

        dag.seal().unwrap();
        drop(node);
        drop(dep);
        drop(dag);
    }

    #[test]
    fn test_quidag_builder_dep_can_be_cloned_into_two_worker_threads() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut dag = QuiDAG::new();
        let resource = dag
            .insert(
                "resource",
                DropSpy {
                    dropped: Arc::clone(&dropped),
                },
            )
            .unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let release_for_build = Arc::clone(&release);
        let resource_for_build = resource.clone();
        let worker = dag
            .node("worker")
            .unwrap()
            .depends_on(&resource)
            .unwrap()
            .build::<WorkerGroup, crate::error::Error, _>(move |deps| {
                // Builder usage contract: declare `worker -> resource` first,
                // then mint one persistent `QuiDep` and clone it into both
                // worker threads. `QuiDAG::drop` will drop `worker` before
                // `resource`, while the `QuiDep` clones keep `resource` alive
                // until the workers have finished.
                let dep = deps.dep(&resource_for_build);
                let mut workers = Vec::new();
                for thread_dep in [dep.clone(), dep] {
                    let started_tx = started_tx.clone();
                    let release = Arc::clone(&release_for_build);
                    workers.push(thread::spawn(move || {
                        started_tx.send(()).unwrap();
                        while !release.load(Ordering::Acquire) {
                            thread::sleep(Duration::from_millis(5));
                        }
                        drop(thread_dep);
                    }));
                }
                Ok(WorkerGroup { workers })
            })
            .unwrap();
        dag.seal().unwrap();

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(!dropped.load(Ordering::Acquire));
        drop(worker);
        drop(resource);

        let (release_start_tx, release_start_rx) = mpsc::channel();
        let (release_done_tx, release_done_rx) = mpsc::channel();
        let release_handle = thread::spawn(move || {
            if release_start_rx.recv().is_ok() {
                release.store(true, Ordering::Release);
                release_done_tx.send(()).unwrap();
            }
        });

        let (drop_done_tx, drop_done_rx) = mpsc::channel();
        let (blocked_tx, blocked_rx) = mpsc::channel();
        let watchdog = thread::spawn(move || {
            let blocked = drop_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err();
            blocked_tx.send(blocked).unwrap();
            if blocked {
                release_start_tx.send(()).unwrap();
            }
        });

        drop(dag);
        let _ = drop_done_tx.send(());

        assert!(blocked_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        release_done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(dropped.load(Ordering::Acquire));

        watchdog.join().unwrap();
        release_handle.join().unwrap();
    }

    #[test]
    fn test_quidag_drop_panics_when_handle_leaked() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut dag = QuiDAG::new();
        let leaked_handle = dag
            .insert(
                "leaked",
                DropSpy {
                    dropped: Arc::clone(&dropped),
                },
            )
            .unwrap();
        dag.seal().unwrap();

        let res = panic::catch_unwind(AssertUnwindSafe(|| drop(dag)));
        assert!(res.is_err());
        drop(leaked_handle);
    }
}
