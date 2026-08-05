use crate::buffer::{
    BufferPool, EvictableBufferPool, FixedBufferPool, PoolGuards, PoolRole, ReadonlyBufferPool,
};
use crate::map::FastHashMap;
use crate::obs;
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use std::any::{Any, TypeId};
use std::fmt::Display;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::forget;
use std::ops::Deref;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::path::PathBuf;
use std::result::Result as StdResult;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::panicking;

/// One lifecycle-managed engine subsystem.
///
/// A component contributes one owned runtime value (`Owned`) and one cloneable
/// dependency handle (`Access`) to the engine registry. The registry stores the
/// owner inside a [`QuiescentBox`], which gives later components a stable
/// address to reference through quiescent guards while still keeping final
/// teardown under registry control.
///
/// Component contract:
/// - [`Self::build`] runs after all earlier dependencies have been registered
///   and may fetch them from [`ComponentRegistry`].
/// - [`Self::build`] registers only its own component. Helper components, such
///   as worker owners, are separate explicit [`RegistryBuilder::build`] steps
///   in the engine's fixed build program.
/// - [`Self::access`] derives the cloneable handle stored in the registry for
///   later dependency lookup.
/// - [`Self::shutdown`] is called in reverse registration order before owner
///   drop. Because shutdown does not receive the registry, components that need
///   other objects during teardown must retain those dependencies in their own
///   owned state.
///
/// Panic safety:
/// - shutdown must close ingress and signal owned workers before a deliberate
///   catchable panic point;
/// - multi-worker owners must attempt every join and required infallible
///   release before propagating the first captured payload;
/// - registry containment uses [`AssertUnwindSafe`] only because the component
///   graph becomes terminal and is never exposed for recovery or reuse;
/// - that containment does not imply that arbitrary component mutation bodies
///   implement `UnwindSafe` or `RefUnwindSafe`; and
/// - normal engine shutdown supplies the documented foreground drain, while
///   bootstrap rollback must establish its own local shutdown preconditions.
pub(crate) trait Component: Sized + 'static {
    type Config;
    type Owned: Send + Sync + 'static;
    type Access: Clone + Send + Sync + 'static;
    /// Failure type produced while constructing this component.
    type Error: Display;

    const NAME: &'static str;

    /// Builds and registers the component's owned runtime state.
    ///
    /// Each implementation retains its narrowest native error until a caller
    /// that combines component domains, such as engine construction, converts
    /// it to the public storage error.
    fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        shelf: ShelfScope<'_, Self>,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Derives the cloneable dependency handle from the registered owner.
    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access;

    /// Stops component-owned work before the owner is dropped.
    fn shutdown(component: &Self::Owned);
}

/// Build-time provision edge from one component to a specific downstream
/// component.
pub(crate) trait Supplier<Down: Component>: Component {
    type Provision: Send + 'static;
}

trait ErasedComponentBox: Send + Sync {
    fn name(&self) -> &'static str;

    fn shutdown(&self);

    fn mark_shutdown_panicked(&self);

    fn shutdown_panicked(&self) -> bool;

    fn outstanding_guard_count(&self) -> usize;
}

struct TypedComponentBox<C: Component> {
    owner: QuiescentBox<C::Owned>,
    shutdown_panicked: AtomicBool,
}

impl<C: Component> ErasedComponentBox for TypedComponentBox<C> {
    #[inline]
    fn name(&self) -> &'static str {
        C::NAME
    }

    #[inline]
    fn shutdown(&self) {
        C::shutdown(&self.owner);
    }

    #[inline]
    fn mark_shutdown_panicked(&self) {
        self.shutdown_panicked.store(true, Ordering::Release);
    }

    #[inline]
    fn shutdown_panicked(&self) -> bool {
        self.shutdown_panicked.load(Ordering::Acquire)
    }

    #[inline]
    fn outstanding_guard_count(&self) -> usize {
        self.owner.outstanding_guard_count()
    }
}

/// Original payload produced by a catchable Rust panic.
pub(crate) type PanicPayload = Box<dyn Any + Send + 'static>;

/// Retains the first panic payload while safely discarding later payloads.
#[derive(Default)]
pub(crate) struct FirstPanic {
    first_payload: Option<PanicPayload>,
    panic_count: usize,
}

impl FirstPanic {
    /// Captures `payload`, retaining the first and forgetting all later ones.
    ///
    /// Forgetting secondary payloads avoids running an arbitrary payload
    /// destructor on an already panic-sensitive teardown path.
    #[inline]
    pub(crate) fn capture(&mut self, payload: PanicPayload) {
        self.panic_count += 1;
        if self.first_payload.is_none() {
            self.first_payload = Some(payload);
        } else {
            forget(payload);
        }
    }

    /// Resumes the first captured panic after local cleanup is complete.
    #[inline]
    pub(crate) fn resume(mut self) {
        if let Some(payload) = self.first_payload.take() {
            resume_unwind(payload);
        }
    }

    #[inline]
    fn into_shutdown_outcome(mut self) -> ComponentShutdownOutcome {
        ComponentShutdownOutcome {
            first_payload: self.first_payload.take(),
            panic_count: self.panic_count,
        }
    }
}

impl Drop for FirstPanic {
    #[inline]
    fn drop(&mut self) {
        if let Some(payload) = self.first_payload.take() {
            forget(payload);
        }
    }
}

/// Aggregate result of one once-only component shutdown dispatch.
#[must_use = "component shutdown panic outcomes must be propagated or suppressed"]
pub(crate) struct ComponentShutdownOutcome {
    first_payload: Option<PanicPayload>,
    panic_count: usize,
}

impl ComponentShutdownOutcome {
    #[inline]
    fn complete() -> Self {
        Self {
            first_payload: None,
            panic_count: 0,
        }
    }

    /// Returns whether one or more component hooks panicked.
    #[inline]
    pub(crate) fn is_degraded(&self) -> bool {
        self.panic_count != 0
    }

    /// Propagates the first payload, or suppresses it during an existing unwind.
    ///
    /// All component hooks and required owner-side terminal transitions must be
    /// complete before this policy is applied.
    #[inline]
    pub(crate) fn propagate_or_suppress(mut self, context: &'static str) {
        let Some(payload) = self.first_payload.take() else {
            return;
        };
        if panicking() {
            obs::error!(
                "event=component_shutdown component=engine action=suppress result=panic context={} panic_count={} reason=thread_already_panicking payload={}",
                context,
                self.panic_count,
                panic_payload_description(payload.as_ref())
            );
            forget(payload);
        } else {
            obs::error!(
                "event=component_shutdown component=engine action=propagate result=panic context={} panic_count={} payload={}",
                context,
                self.panic_count,
                panic_payload_description(payload.as_ref())
            );
            resume_unwind(payload);
        }
    }

    #[cfg(test)]
    #[inline]
    fn panic_count(&self) -> usize {
        self.panic_count
    }
}

impl Drop for ComponentShutdownOutcome {
    #[inline]
    fn drop(&mut self) {
        if let Some(payload) = self.first_payload.take() {
            forget(payload);
        }
    }
}

/// Ordered registry for engine components and their dependency handles.
///
/// The registry intentionally models lifecycle as one fixed topological order
/// rather than a general dependency DAG. Registration order defines:
/// - which dependencies are available to later [`Component::build`] calls
/// - reverse-order explicit shutdown in [`Self::shutdown_all`]
/// - reverse-order final owner drop in [`Drop`]
///
/// Current engine registration order:
/// 1. `StorageRootLease`
/// 2. `EnginePoisoner`
/// 3. `MandatoryRuntime` -> `EnginePoisoner`
/// 4. `FileSystem`
/// 5. `DiskPool` -> `FileSystem`
/// 6. `MetaPool`
/// 7. `IndexPool` -> `FileSystem`
/// 8. `MemPool` -> `FileSystem`
/// 9. `FileSystemWorkers` -> `EnginePoisoner`, `FileSystem`, `IndexPool`,
///    `MemPool`
/// 10. `SharedPoolEvictorWorkers` -> `DiskPool`, `IndexPool`, `MemPool`
/// 11. `LockManager`
/// 12. `Catalog` -> `EnginePoisoner`, `MetaPool`, `FileSystem`, `DiskPool`
/// 13. `TransactionSystem` -> `EnginePoisoner`, `MandatoryRuntime`,
///     `MetaPool`, `IndexPool`, `MemPool`, `FileSystem`, `DiskPool`, `Catalog`
/// 14. `TransactionPurgeWorkers` -> `TransactionSystem`
/// 15. `MandatoryRuntimeWorkers` -> `MandatoryRuntime`
/// 16. `TransactionRedoWorkers` -> `TransactionSystem`
///
/// Every entry above is an explicit [`RegistryBuilder::build`] call in the
/// engine build program; component builds do not invoke downstream component
/// builds themselves.
/// `StorageRootLease` intentionally has no runtime dependency handle. Its first
/// position makes it the final shutdown target, bracketing all subordinate
/// storage activity with canonical-root ownership.
///
/// Each arrow points from a component to a direct registry dependency fetched
/// during its build. `FileSystemWorkers` also consumes shelf provisions from
/// `FileSystem`, `IndexPool`, and `MemPool`; the transaction system supplies
/// independent purge and redo startup resources, and the early mandatory
/// runtime supplies deferred runner-startup state. Those build-time provision
/// edges are transferred through [`ShelfScope`] rather than fetched from the
/// registry.
///
/// In addition to the direct component edges above, `Catalog` owns user-table
/// runtimes that retain guards into `MemPool`, `IndexPool`, `FileSystem`,
/// and `DiskPool`. That is why `Catalog` must be registered after those pool
/// and file components: reverse shutdown and drop must release table-owned
/// guards before the underlying pool owners are torn down.
///
/// Worker components may also retain extra shutdown-only dependencies in their
/// owned state, but those retained guards are an implementation detail rather
/// than additional registry lookup edges.
pub(crate) struct ComponentRegistry {
    access_map: FastHashMap<TypeId, Box<dyn Any + Send + Sync>>,
    boxed_vec: Vec<Box<dyn ErasedComponentBox>>,
    shutdown_started: AtomicBool,
    shutdown_degraded: AtomicBool,
}

impl Default for ComponentRegistry {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl ComponentRegistry {
    /// Create an empty component registry.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            access_map: FastHashMap::default(),
            boxed_vec: Vec::new(),
            shutdown_started: AtomicBool::new(false),
            shutdown_degraded: AtomicBool::new(false),
        }
    }

    /// Register one component owner and publish its dependency handle.
    ///
    /// Registration order is significant: later components may only depend on
    /// components that were registered earlier, and reverse registration order
    /// becomes the engine shutdown/drop order.
    ///
    /// # Panics
    ///
    /// Panics when the component name is empty or the fixed engine build
    /// program registers the same component type more than once.
    #[inline]
    pub(crate) fn register<C: Component>(&mut self, owned: C::Owned) {
        assert!(
            !C::NAME.is_empty(),
            "component implementations must provide a stable non-empty name"
        );
        let tid = TypeId::of::<C>();
        assert!(
            !self.access_map.contains_key(&tid),
            "component {} is already registered in the fixed engine build order",
            C::NAME
        );

        let owner = QuiescentBox::new(owned);
        let access = C::access(&owner);
        self.access_map.insert(tid, Box::new(access));
        self.boxed_vec.push(Box::new(TypedComponentBox::<C> {
            owner,
            shutdown_panicked: AtomicBool::new(false),
        }));
    }

    /// Return the cloned dependency handle for a previously registered
    /// component, if present.
    #[inline]
    pub(crate) fn get<C: Component>(&self) -> Option<C::Access> {
        self.access_map
            .get(&TypeId::of::<C>())
            .and_then(|v| v.downcast_ref::<C::Access>())
            .cloned()
    }

    /// Fetch a required dependency handle for a previously registered
    /// component.
    ///
    /// # Panics
    ///
    /// Panics when the fixed engine registration order has not registered the
    /// requested component with the expected access type.
    #[inline]
    pub(crate) fn dependency<C: Component>(&self) -> C::Access {
        self.get::<C>().unwrap_or_else(|| {
            panic!(
                "component {} is missing from the fixed engine registration order",
                C::NAME
            )
        })
    }

    /// Run explicit component shutdown in reverse registration order.
    ///
    /// This is idempotent at the registry level and is intended to stop worker
    /// activity before owner drop starts waiting on quiescent guards. Catchable
    /// hook panics are contained independently so every later hook still runs
    /// once. The returned outcome retains the first original payload.
    #[inline]
    pub(crate) fn shutdown_all(&self) -> ComponentShutdownOutcome {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return ComponentShutdownOutcome::complete();
        }
        let mut panics = FirstPanic::default();
        for component in self.boxed_vec.iter().rev() {
            let component_name = component.name();
            obs::info!(
                "event=component_lifecycle component=engine storage_component={} action=shutdown_start result=ok",
                component_name
            );
            // The graph is terminal after shutdown starts. This assertion is
            // scoped to hook dispatch and is not a claim that arbitrary
            // component-domain mutation is unwind-safe.
            match catch_unwind(AssertUnwindSafe(|| component.shutdown())) {
                Ok(()) => {
                    obs::info!(
                        "event=component_lifecycle component=engine storage_component={} action=shutdown_finish result=ok",
                        component_name
                    );
                }
                Err(payload) => {
                    component.mark_shutdown_panicked();
                    self.shutdown_degraded.store(true, Ordering::Release);
                    obs::error!(
                        "event=component_lifecycle component=engine storage_component={} action=shutdown_finish result=panic payload={}",
                        component_name,
                        panic_payload_description(payload.as_ref())
                    );
                    panics.capture(payload);
                }
            }
        }
        panics.into_shutdown_outcome()
    }
}

impl Drop for ComponentRegistry {
    #[inline]
    fn drop(&mut self) {
        self.access_map.clear();
        if !self.shutdown_degraded.load(Ordering::Acquire) {
            while let Some(owner) = self.boxed_vec.pop() {
                drop(owner);
            }
            return;
        }

        while let Some(owner) = self.boxed_vec.pop() {
            let guard_count = owner.outstanding_guard_count();
            let reason = if owner.shutdown_panicked() {
                Some("shutdown_panic")
            } else if guard_count != 0 {
                Some("outstanding_guards")
            } else {
                None
            };
            if let Some(reason) = reason {
                obs::error!(
                    "event=component_owner component=engine storage_component={} action=leak result=degraded reason={} guard_count={}",
                    owner.name(),
                    reason,
                    guard_count
                );
                forget(owner);
            } else {
                drop(owner);
            }
        }
    }
}

type ShelfKey = (TypeId, TypeId);

/// Build-only storage for transient provisions passed between components.
pub(crate) struct Shelf {
    parts: FastHashMap<ShelfKey, Box<dyn Any + Send>>,
}

impl Shelf {
    /// Creates an empty build shelf.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            parts: FastHashMap::default(),
        }
    }

    fn key<Up: Component, Down: Component>() -> ShelfKey {
        (TypeId::of::<Up>(), TypeId::of::<Down>())
    }

    fn put<Up, Down>(&mut self, provision: <Up as Supplier<Down>>::Provision)
    where
        Up: Supplier<Down>,
        Down: Component,
    {
        let key = Self::key::<Up, Down>();
        assert!(
            !self.parts.contains_key(&key),
            "component provision edge {} -> {} was published more than once",
            Up::NAME,
            Down::NAME
        );
        self.parts.insert(key, Box::new(provision));
    }

    fn take<Up, Down>(&mut self) -> <Up as Supplier<Down>>::Provision
    where
        Up: Supplier<Down>,
        Down: Component,
    {
        let provision = self
            .parts
            .remove(&Self::key::<Up, Down>())
            .unwrap_or_else(|| {
                panic!(
                    "component provision edge {} -> {} is missing from the fixed engine build order",
                    Up::NAME,
                    Down::NAME
                )
            });
        *provision
            .downcast::<<Up as Supplier<Down>>::Provision>()
            .unwrap_or_else(|_| {
                panic!(
                    "invalid shelf provision type for edge {} -> {}",
                    Up::NAME,
                    Down::NAME
                )
            })
    }

    /// Returns whether the shelf still contains build provisions.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    #[inline]
    fn clear(&mut self) {
        self.parts.clear();
    }

    /// Creates a component-scoped shelf view.
    #[inline]
    pub(crate) fn scope<C: Component>(&mut self) -> ShelfScope<'_, C> {
        ShelfScope {
            shelf: self,
            _marker: PhantomData,
        }
    }
}

/// Component-scoped view over the shared build shelf.
pub(crate) struct ShelfScope<'a, C> {
    shelf: &'a mut Shelf,
    _marker: PhantomData<fn() -> C>,
}

impl<'a, C: Component> ShelfScope<'a, C> {
    /// Stores a provision from the scoped component to a downstream component.
    ///
    /// # Panics
    ///
    /// Panics when the fixed engine build program publishes the typed edge
    /// from `C` to `Down` more than once.
    #[inline]
    pub(crate) fn put<Down>(&mut self, provision: <C as Supplier<Down>>::Provision)
    where
        C: Supplier<Down>,
        Down: Component,
    {
        self.shelf.put::<C, Down>(provision)
    }

    /// Takes a provision supplied by an upstream component.
    ///
    /// # Panics
    ///
    /// Panics when the fixed engine build program has not published the typed
    /// edge from `Up` to `C`, or if its stored type is inconsistent.
    #[inline]
    pub(crate) fn take<Up>(&mut self) -> <Up as Supplier<C>>::Provision
    where
        Up: Supplier<C>,
    {
        self.shelf.take::<Up, C>()
    }
}

/// Build-phase owner for the runtime registry and transient provisions.
pub(crate) struct RegistryBuilder {
    registry: Option<ComponentRegistry>,
    shelf: Shelf,
}

impl RegistryBuilder {
    /// Creates a builder for component registration.
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            registry: Some(ComponentRegistry::new()),
            shelf: Shelf::new(),
        }
    }

    /// Builds one component with the provided config.
    #[inline]
    pub(crate) async fn build<C: Component>(
        &mut self,
        config: C::Config,
    ) -> StdResult<(), C::Error> {
        let registry = self
            .registry
            .as_mut()
            .expect("registry builder is armed until finish");
        let shelf = self.shelf.scope::<C>();
        obs::info!(
            "event=component_lifecycle component=engine storage_component={} action=build_start result=ok",
            C::NAME
        );
        C::build(config, registry, shelf)
            .await
            .inspect(|_| {
                obs::info!(
                    "event=component_lifecycle component=engine storage_component={} action=build_finish result=ok",
                    C::NAME
                );
            })
            .inspect_err(|err| {
                obs::error!(
                    "event=component_lifecycle component=engine storage_component={} action=build_finish result=error error={}",
                    C::NAME,
                    err
                );
            })
    }

    /// Finishes the builder and returns the completed registry.
    ///
    /// # Panics
    ///
    /// Panics when a fixed provision edge was not consumed or when the
    /// builder's registry was unexpectedly disarmed before this consuming
    /// call.
    #[inline]
    pub(crate) fn finish(mut self) -> ComponentRegistry {
        assert!(
            self.shelf.is_empty(),
            "component shelf still contains unconsumed provisions after the fixed engine build"
        );
        self.registry
            .take()
            .expect("registry builder must remain armed until finish")
    }
}

impl Drop for RegistryBuilder {
    #[inline]
    fn drop(&mut self) {
        let outcome = self.registry.as_ref().map(ComponentRegistry::shutdown_all);
        // Provisions can retain quiescent guards into registered owners, so the
        // shelf must be drained before owner drop starts.
        self.shelf.clear();
        if let Some(outcome) = outcome {
            outcome.propagate_or_suppress("registry_builder_drop");
        }
    }
}

macro_rules! pool_access_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(Clone)]
        pub(crate) struct $name(QuiescentGuard<$inner>);

        impl $name {
            #[inline]
            pub(crate) fn clone_inner(&self) -> QuiescentGuard<$inner> {
                self.0.clone()
            }

            #[inline]
            #[expect(dead_code, reason = "reserved into_inner")]
            pub(crate) fn into_inner(self) -> QuiescentGuard<$inner> {
                self.0
            }
        }

        impl From<QuiescentGuard<$inner>> for $name {
            #[inline]
            fn from(value: QuiescentGuard<$inner>) -> Self {
                Self(value)
            }
        }

        impl Deref for $name {
            type Target = $inner;

            #[inline]
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

pool_access_newtype!(MetaPool, FixedBufferPool);
pool_access_newtype!(IndexPool, EvictableBufferPool);
pool_access_newtype!(MemPool, EvictableBufferPool);
pool_access_newtype!(DiskPool, ReadonlyBufferPool);

/// Canonical engine buffer-pool capability shared by session runtime work.
pub(crate) struct EnginePools {
    /// Metadata pool used for catalog and block-index pages.
    pub(crate) meta: QuiescentGuard<FixedBufferPool>,
    /// Secondary-index pool.
    pub(crate) index: QuiescentGuard<EvictableBufferPool>,
    /// Mutable row-page pool.
    pub(crate) mem: QuiescentGuard<EvictableBufferPool>,
    /// Readonly persisted-page pool.
    pub(crate) disk: QuiescentGuard<ReadonlyBufferPool>,
    /// Prebuilt guards for the exact four pool identities above.
    guards: PoolGuards,
}

impl Clone for EnginePools {
    #[inline]
    fn clone(&self) -> Self {
        Self::new(
            self.meta.clone(),
            self.index.clone(),
            self.mem.clone(),
            self.disk.clone(),
        )
    }
}

impl EnginePools {
    /// Create a bundle from explicit inner pool handles.
    #[inline]
    pub(crate) fn new(
        meta: QuiescentGuard<FixedBufferPool>,
        index: QuiescentGuard<EvictableBufferPool>,
        mem: QuiescentGuard<EvictableBufferPool>,
        disk: QuiescentGuard<ReadonlyBufferPool>,
    ) -> Self {
        let guards = PoolGuards::builder()
            .push(PoolRole::Meta, meta.pool_guard())
            .push(PoolRole::Index, index.pool_guard())
            .push(PoolRole::Mem, mem.pool_guard())
            .push(PoolRole::Disk, disk.pool_guard())
            .build();
        Self {
            meta,
            index,
            mem,
            disk,
            guards,
        }
    }

    /// Borrow the canonical guard bundle for all engine buffer pools.
    #[inline]
    pub(crate) fn pool_guards(&self) -> &PoolGuards {
        &self.guards
    }
}

/// Configuration for the metadata buffer pool.
#[derive(Clone, Copy)]
pub(crate) struct MetaPoolConfig {
    /// Number of bytes reserved for metadata pages.
    pub(crate) bytes: usize,
}

impl MetaPoolConfig {
    /// Creates metadata pool config with the given byte capacity.
    #[inline]
    pub(crate) fn new(bytes: usize) -> Self {
        Self { bytes }
    }
}

/// Configuration for the evictable index buffer pool.
#[derive(Clone)]
pub(crate) struct IndexPoolConfig {
    /// Number of bytes reserved for index pages.
    pub(crate) bytes: usize,
    /// Swap file path backing evicted index pages.
    pub(crate) swap_file: PathBuf,
    /// Maximum size of the swap file in bytes.
    pub(crate) max_file_size: usize,
}

impl IndexPoolConfig {
    /// Creates index pool config with byte capacity, swap path, and file limit.
    #[inline]
    pub(crate) fn new(bytes: usize, swap_file: impl Into<PathBuf>, max_file_size: usize) -> Self {
        Self {
            bytes,
            swap_file: swap_file.into(),
            max_file_size,
        }
    }
}

/// Configuration for the readonly disk buffer pool.
#[derive(Clone, Copy)]
pub(crate) struct DiskPoolConfig {
    /// Number of bytes reserved for cached disk pages.
    pub(crate) bytes: usize,
}

impl DiskPoolConfig {
    /// Creates disk pool config with the given byte capacity.
    #[inline]
    pub(crate) fn new(bytes: usize) -> Self {
        Self { bytes }
    }
}

/// Renders a panic payload without consuming it.
#[inline]
pub(crate) fn panic_payload_description(payload: &(dyn Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else {
        "<opaque>"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{DiscloseError, ErrorKind, RuntimeError};
    use error_stack::Report;
    use parking_lot::Mutex;
    use std::convert::Infallible;
    use std::panic::{self, AssertUnwindSafe};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    struct ValueComponent;

    impl Component for ValueComponent {
        type Config = usize;
        type Owned = usize;
        type Access = usize;
        type Error = Infallible;

        const NAME: &'static str = "value";

        #[inline]
        async fn build(
            config: Self::Config,
            registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> StdResult<(), Self::Error> {
            registry.register::<Self>(config);
            Ok(())
        }

        #[inline]
        fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
            **owner
        }

        #[inline]
        fn shutdown(_component: &Self::Owned) {}
    }

    struct FailingBuildOwned {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Drop for FailingBuildOwned {
        fn drop(&mut self) {
            self.events.lock().push("drop");
        }
    }

    struct FailingBuildComponent;

    impl Component for FailingBuildComponent {
        type Config = Arc<Mutex<Vec<&'static str>>>;
        type Owned = FailingBuildOwned;
        type Access = ();
        type Error = Report<RuntimeError>;

        const NAME: &'static str = "failing-build";

        async fn build(
            events: Self::Config,
            registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> StdResult<(), Self::Error> {
            registry.register::<Self>(FailingBuildOwned { events });
            Err(Report::new(RuntimeError::BackgroundSpawn))
        }

        fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

        fn shutdown(component: &Self::Owned) {
            component.events.lock().push("shutdown");
        }
    }

    struct ShutdownProbe {
        events: Arc<Mutex<Vec<&'static str>>>,
        panic_message: Option<&'static str>,
        drop_event: &'static str,
    }

    impl Drop for ShutdownProbe {
        fn drop(&mut self) {
            self.events.lock().push(self.drop_event);
        }
    }

    struct ShutdownA;
    struct ShutdownB;
    struct ShutdownC;

    macro_rules! shutdown_component {
        ($component:ident, $name:literal) => {
            impl Component for $component {
                type Config = ();
                type Owned = ShutdownProbe;
                type Access = ();
                type Error = Infallible;

                const NAME: &'static str = $name;

                #[inline]
                async fn build(
                    _config: Self::Config,
                    _registry: &mut ComponentRegistry,
                    _shelf: ShelfScope<'_, Self>,
                ) -> StdResult<(), Self::Error> {
                    unreachable!("test-only component")
                }

                #[inline]
                fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

                #[inline]
                fn shutdown(component: &Self::Owned) {
                    component.events.lock().push($name);
                    if let Some(message) = component.panic_message {
                        panic::panic_any(message);
                    }
                }
            }
        };
    }

    shutdown_component!(ShutdownA, "a");
    shutdown_component!(ShutdownB, "b");
    shutdown_component!(ShutdownC, "c");

    #[derive(Clone)]
    struct AccessProbe {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Drop for AccessProbe {
        fn drop(&mut self) {
            self.events.lock().push("access");
        }
    }

    struct OwnerProbe {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl Drop for OwnerProbe {
        fn drop(&mut self) {
            self.events.lock().push("owner");
        }
    }

    struct AccessOrderComponent;

    impl Component for AccessOrderComponent {
        type Config = ();
        type Owned = OwnerProbe;
        type Access = AccessProbe;
        type Error = Infallible;

        const NAME: &'static str = "access-order";

        #[inline]
        async fn build(
            _config: Self::Config,
            _registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> StdResult<(), Self::Error> {
            unreachable!("test-only component")
        }

        #[inline]
        fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
            AccessProbe {
                events: Arc::clone(&owner.events),
            }
        }

        #[inline]
        fn shutdown(_component: &Self::Owned) {}
    }

    #[test]
    fn test_component_registry_returns_typed_access_clone() {
        let mut registry = ComponentRegistry::new();
        registry.register::<ValueComponent>(7);
        assert_eq!(registry.get::<ValueComponent>(), Some(7));
        assert_eq!(registry.dependency::<ValueComponent>(), 7);
    }

    #[test]
    fn test_registry_builder_finishes_normal_component_build() {
        smol::block_on(async {
            let mut builder = RegistryBuilder::new();
            builder.build::<ValueComponent>(7).await.unwrap();

            let registry = builder.finish();
            assert_eq!(registry.dependency::<ValueComponent>(), 7);
        });
    }

    #[test]
    fn test_registry_builder_cleans_up_after_component_build_failure() {
        let events = Arc::new(Mutex::new(Vec::new()));
        {
            let mut builder = RegistryBuilder::new();
            let err = smol::block_on(builder.build::<FailingBuildComponent>(Arc::clone(&events)))
                .unwrap_err();
            assert_eq!(err.current_context(), &RuntimeError::BackgroundSpawn);
            let err = err.disclose();
            assert_eq!(err.kind(), ErrorKind::Runtime);
            assert_eq!(
                err.report().downcast_ref::<RuntimeError>().copied(),
                Some(RuntimeError::BackgroundSpawn)
            );
        }

        assert_eq!(events.lock().as_slice(), &["shutdown", "drop"]);
    }

    #[test]
    fn test_component_registry_shutdown_uses_reverse_registration_order_and_is_idempotent() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ComponentRegistry::new();
        registry.register::<ShutdownA>(ShutdownProbe {
            events: Arc::clone(&events),
            panic_message: None,
            drop_event: "drop-a",
        });
        registry.register::<ShutdownB>(ShutdownProbe {
            events: Arc::clone(&events),
            panic_message: None,
            drop_event: "drop-b",
        });
        registry.register::<ShutdownC>(ShutdownProbe {
            events: Arc::clone(&events),
            panic_message: None,
            drop_event: "drop-c",
        });

        let outcome = registry.shutdown_all();
        assert!(!outcome.is_degraded());
        outcome.propagate_or_suppress("component_registry_test");
        let repeated = registry.shutdown_all();
        assert!(!repeated.is_degraded());
        repeated.propagate_or_suppress("component_registry_test");

        assert_eq!(events.lock().as_slice(), &["c", "b", "a"]);
        drop(registry);
        assert_eq!(
            events.lock().as_slice(),
            &["c", "b", "a", "drop-c", "drop-b", "drop-a"]
        );
    }

    #[test]
    fn test_component_registry_contains_all_hook_panics_and_resumes_first_payload() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ComponentRegistry::new();
        registry.register::<ShutdownA>(ShutdownProbe {
            events: Arc::clone(&events),
            panic_message: None,
            drop_event: "drop-a",
        });
        registry.register::<ShutdownB>(ShutdownProbe {
            events: Arc::clone(&events),
            panic_message: Some("second panic"),
            drop_event: "drop-b",
        });
        registry.register::<ShutdownC>(ShutdownProbe {
            events: Arc::clone(&events),
            panic_message: Some("first panic"),
            drop_event: "drop-c",
        });

        let outcome = registry.shutdown_all();
        assert!(outcome.is_degraded());
        assert_eq!(outcome.panic_count(), 2);
        assert_eq!(events.lock().as_slice(), &["c", "b", "a"]);

        let repeated = registry.shutdown_all();
        assert!(!repeated.is_degraded());
        repeated.propagate_or_suppress("component_registry_repeated_test");

        let payload = panic::catch_unwind(AssertUnwindSafe(|| {
            outcome.propagate_or_suppress("component_registry_test");
        }))
        .unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("first panic")
        );
    }

    #[test]
    fn test_first_panic_forgets_secondary_payload_without_running_destructor() {
        struct PanicOnDrop;

        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("secondary panic payload destructor must not run");
            }
        }

        let mut panics = FirstPanic::default();
        panics.capture(Box::new("first panic"));
        panics.capture(Box::new(PanicOnDrop));
        let payload = panic::catch_unwind(AssertUnwindSafe(|| panics.resume())).unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("first panic")
        );
    }

    #[test]
    fn test_component_shutdown_payload_is_suppressed_during_existing_unwind() {
        struct ApplyOutcomeOnDrop(Option<ComponentShutdownOutcome>);

        impl Drop for ApplyOutcomeOnDrop {
            fn drop(&mut self) {
                self.0
                    .take()
                    .expect("test shutdown outcome")
                    .propagate_or_suppress("existing_unwind_test");
            }
        }

        let events = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ComponentRegistry::new();
        registry.register::<ShutdownA>(ShutdownProbe {
            events,
            panic_message: Some("shutdown panic"),
            drop_event: "drop-a",
        });
        let outcome = registry.shutdown_all();

        let payload = panic::catch_unwind(AssertUnwindSafe(|| {
            let _apply = ApplyOutcomeOnDrop(Some(outcome));
            panic::panic_any("outer panic");
        }))
        .unwrap_err();
        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("outer panic")
        );
    }

    #[test]
    fn test_component_registry_drop_clears_access_before_owner_drop() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ComponentRegistry::new();
        registry.register::<AccessOrderComponent>(OwnerProbe {
            events: Arc::clone(&events),
        });

        drop(registry);

        assert_eq!(events.lock().as_slice(), &["access", "owner"]);
    }

    struct CountedOwner {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for CountedOwner {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    struct DependencyComponent;
    struct IndependentComponent;
    struct ShelfGuardConsumer;
    struct ShelfPanicComponent;
    struct SuspectComponent;

    struct SuspectOwned {
        _dependency: QuiescentGuard<CountedOwner>,
        _owner: CountedOwner,
    }

    macro_rules! counted_component {
        ($component:ident, $access:ty, $access_expr:expr, $shutdown:block) => {
            impl Component for $component {
                type Config = ();
                type Owned = CountedOwner;
                type Access = $access;
                type Error = Infallible;

                const NAME: &'static str = stringify!($component);

                async fn build(
                    _config: Self::Config,
                    _registry: &mut ComponentRegistry,
                    _shelf: ShelfScope<'_, Self>,
                ) -> StdResult<(), Self::Error> {
                    unreachable!("test-only component")
                }

                fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
                    ($access_expr)(owner)
                }

                fn shutdown(_component: &Self::Owned) $shutdown
            }
        };
    }

    counted_component!(
        DependencyComponent,
        QuiescentGuard<CountedOwner>,
        |owner: &QuiescentBox<CountedOwner>| owner.guard(),
        {}
    );
    counted_component!(
        IndependentComponent,
        (),
        |_owner: &QuiescentBox<CountedOwner>| (),
        {}
    );

    counted_component!(
        ShelfPanicComponent,
        (),
        |_owner: &QuiescentBox<CountedOwner>| (),
        {
            panic!("builder shelf shutdown panic");
        }
    );

    impl Component for ShelfGuardConsumer {
        type Config = ();
        type Owned = ();
        type Access = ();
        type Error = Infallible;

        const NAME: &'static str = "ShelfGuardConsumer";

        async fn build(
            _config: Self::Config,
            _registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> StdResult<(), Self::Error> {
            unreachable!("test-only component")
        }

        fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

        fn shutdown(_component: &Self::Owned) {}
    }

    impl Supplier<ShelfGuardConsumer> for DependencyComponent {
        type Provision = QuiescentGuard<CountedOwner>;
    }

    impl Component for SuspectComponent {
        type Config = ();
        type Owned = SuspectOwned;
        type Access = ();
        type Error = Infallible;

        const NAME: &'static str = "SuspectComponent";

        async fn build(
            _config: Self::Config,
            _registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> StdResult<(), Self::Error> {
            unreachable!("test-only component")
        }

        fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

        fn shutdown(_component: &Self::Owned) {
            panic!("suspect shutdown");
        }
    }

    #[test]
    fn test_degraded_registry_drop_leaks_only_suspect_guard_closure() {
        let dependency_dropped = Arc::new(AtomicUsize::new(0));
        let independent_dropped = Arc::new(AtomicUsize::new(0));
        let suspect_dropped = Arc::new(AtomicUsize::new(0));
        let mut registry = ComponentRegistry::new();
        registry.register::<DependencyComponent>(CountedOwner {
            dropped: Arc::clone(&dependency_dropped),
        });
        let external_guard = registry.dependency::<DependencyComponent>();
        registry.register::<IndependentComponent>(CountedOwner {
            dropped: Arc::clone(&independent_dropped),
        });
        registry.register::<SuspectComponent>(SuspectOwned {
            _dependency: external_guard.clone(),
            _owner: CountedOwner {
                dropped: Arc::clone(&suspect_dropped),
            },
        });

        let outcome = registry.shutdown_all();
        assert!(outcome.is_degraded());
        drop(outcome);
        drop(registry);

        assert_eq!(independent_dropped.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(suspect_dropped.load(AtomicOrdering::Relaxed), 0);
        assert_eq!(dependency_dropped.load(AtomicOrdering::Relaxed), 0);
        // The dependency allocation was intentionally leaked, so a guard
        // retained past registry destruction can still release safely.
        drop(external_guard);
    }

    #[test]
    fn test_builder_clears_shelf_guards_before_degraded_registry_drop() {
        let dependency_dropped = Arc::new(AtomicUsize::new(0));
        let suspect_dropped = Arc::new(AtomicUsize::new(0));
        let payload = panic::catch_unwind(AssertUnwindSafe(|| {
            let mut builder = RegistryBuilder::new();
            let shelf_guard = {
                let registry = builder
                    .registry
                    .as_mut()
                    .expect("test builder retains registry");
                registry.register::<DependencyComponent>(CountedOwner {
                    dropped: Arc::clone(&dependency_dropped),
                });
                registry.dependency::<DependencyComponent>()
            };
            builder
                .shelf
                .scope::<DependencyComponent>()
                .put::<ShelfGuardConsumer>(shelf_guard);
            builder
                .registry
                .as_mut()
                .expect("test builder retains registry")
                .register::<ShelfPanicComponent>(CountedOwner {
                    dropped: Arc::clone(&suspect_dropped),
                });
            drop(builder);
        }))
        .unwrap_err();

        assert_eq!(
            payload.downcast_ref::<&'static str>().copied(),
            Some("builder shelf shutdown panic")
        );
        assert_eq!(dependency_dropped.load(AtomicOrdering::Relaxed), 1);
        assert_eq!(suspect_dropped.load(AtomicOrdering::Relaxed), 0);
    }

    struct Upstream;
    struct Downstream;

    impl Component for Upstream {
        type Config = ();
        type Owned = ();
        type Access = ();
        type Error = Infallible;

        const NAME: &'static str = "up";

        async fn build(
            _config: Self::Config,
            _registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> StdResult<(), Self::Error> {
            unreachable!("test-only component")
        }

        fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

        fn shutdown(_component: &Self::Owned) {}
    }

    impl Component for Downstream {
        type Config = ();
        type Owned = ();
        type Access = ();
        type Error = Infallible;

        const NAME: &'static str = "down";

        async fn build(
            _config: Self::Config,
            _registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> StdResult<(), Self::Error> {
            unreachable!("test-only component")
        }

        fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

        fn shutdown(_component: &Self::Owned) {}
    }

    impl Supplier<Downstream> for Upstream {
        type Provision = usize;
    }

    #[test]
    fn test_shelf_take_removes_edge_entry() {
        let mut shelf = Shelf::new();
        {
            let mut up = shelf.scope::<Upstream>();
            up.put::<Downstream>(7);
        }
        {
            let mut down = shelf.scope::<Downstream>();
            assert_eq!(down.take::<Upstream>(), 7);
        }
        assert!(shelf.is_empty());
    }
}
