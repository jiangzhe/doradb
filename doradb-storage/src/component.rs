use crate::buffer::{EvictableBufferPool, FixedBufferPool, GlobalReadonlyBufferPool};
use crate::error::{Error, Result};
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};

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
/// - [`Self::build`] may register additional helper components, such as worker
///   components, as long as the overall registration order remains a valid
///   dependency order.
/// - [`Self::access`] derives the cloneable handle stored in the registry for
///   later dependency lookup.
/// - [`Self::shutdown`] is called in reverse registration order before owner
///   drop. Because shutdown does not receive the registry, components that need
///   other objects during teardown must retain those dependencies in their own
///   owned state.
pub(crate) trait Component: Sized + 'static {
    type Config;
    type Owned: Send + Sync + 'static;
    type Access: Clone + Send + Sync + 'static;

    const NAME: &'static str;

    fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
        shelf: ShelfScope<'_, Self>,
    ) -> impl Future<Output = Result<()>> + Send;

    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access;

    fn shutdown(component: &Self::Owned);
}

/// Build-time provision edge from one component to a specific downstream
/// component.
pub(crate) trait Supplier<Down: Component>: Component {
    type Provision: Send + 'static;
}

trait ErasedComponentBox: Send + Sync {
    fn shutdown(&self);
}

struct TypedComponentBox<C: Component> {
    owner: QuiescentBox<C::Owned>,
}

impl<C: Component> ErasedComponentBox for TypedComponentBox<C> {
    #[inline]
    fn shutdown(&self) {
        C::shutdown(&self.owner);
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
/// 1. `DiskPool`
/// 2. `DiskPoolWorkers` -> `DiskPool`
/// 3. `TableFileSystem`
/// 4. `TableFileSystemWorkers` -> `TableFileSystem`
/// 5. `MetaPool`
/// 6. `IndexPool`
/// 7. `MemPool`
/// 8. `MemPoolWorkers` -> `MemPool`
/// 9. `Catalog` -> `MetaPool`, `IndexPool`, `TableFileSystem`, `DiskPool`
/// 10. `TransactionSystem` -> `MetaPool`, `IndexPool`, `MemPool`,
///     `TableFileSystem`, `DiskPool`, `Catalog`
/// 11. `TransactionSystemWorkers` -> `TransactionSystem`
///
/// In addition to the direct component edges above, `Catalog` owns user-table
/// runtimes that retain guards into `MemPool`, `IndexPool`, `TableFileSystem`,
/// and `DiskPool`. That is why `Catalog` must be registered after those pool
/// and file components: reverse shutdown and drop must release table-owned
/// guards before the underlying pool owners are torn down.
///
/// Worker components may also retain extra shutdown-only dependencies in their
/// owned state, but those retained guards are an implementation detail rather
/// than additional registry lookup edges.
pub(crate) struct ComponentRegistry {
    access_map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    boxed_vec: Vec<Box<dyn ErasedComponentBox>>,
    shutdown_started: AtomicBool,
}

type ShelfKey = (TypeId, TypeId);

/// Build-only storage for transient provisions passed between components.
pub(crate) struct Shelf {
    parts: HashMap<ShelfKey, Box<dyn Any + Send>>,
}

impl Shelf {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            parts: HashMap::new(),
        }
    }

    #[inline]
    fn key<Up: Component, Down: Component>() -> ShelfKey {
        (TypeId::of::<Up>(), TypeId::of::<Down>())
    }

    #[inline]
    fn put<Up, Down>(&mut self, provision: <Up as Supplier<Down>>::Provision) -> Result<()>
    where
        Up: Supplier<Down>,
        Down: Component,
    {
        let old = self
            .parts
            .insert(Self::key::<Up, Down>(), Box::new(provision));
        if old.is_some() {
            return Err(Error::InvalidState);
        }
        Ok(())
    }

    #[inline]
    fn take<Up, Down>(&mut self) -> Option<<Up as Supplier<Down>>::Provision>
    where
        Up: Supplier<Down>,
        Down: Component,
    {
        self.parts
            .remove(&Self::key::<Up, Down>())
            .map(|provision| {
                *provision
                    .downcast::<<Up as Supplier<Down>>::Provision>()
                    .unwrap_or_else(|_| {
                        panic!(
                            "invalid shelf provision type for edge {} -> {}",
                            Up::NAME,
                            Down::NAME
                        )
                    })
            })
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    #[inline]
    fn clear(&mut self) {
        self.parts.clear();
    }

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
    #[inline]
    pub(crate) fn put<Down>(&mut self, provision: <C as Supplier<Down>>::Provision) -> Result<()>
    where
        C: Supplier<Down>,
        Down: Component,
    {
        self.shelf.put::<C, Down>(provision)
    }

    #[inline]
    pub(crate) fn take<Up>(&mut self) -> Option<<Up as Supplier<C>>::Provision>
    where
        Up: Supplier<C>,
    {
        self.shelf.take::<Up, C>()
    }

    #[inline]
    pub(crate) fn scope<Other: Component>(&mut self) -> ShelfScope<'_, Other> {
        self.shelf.scope::<Other>()
    }
}

/// Build-phase owner for the runtime registry and transient provisions.
pub(crate) struct RegistryBuilder {
    registry: Option<ComponentRegistry>,
    shelf: Shelf,
}

impl RegistryBuilder {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            registry: Some(ComponentRegistry::new()),
            shelf: Shelf::new(),
        }
    }

    #[inline]
    pub(crate) async fn build<C: Component>(&mut self, config: C::Config) -> Result<()> {
        let registry = self
            .registry
            .as_mut()
            .expect("registry builder is armed until finish");
        let shelf = self.shelf.scope::<C>();
        C::build(config, registry, shelf).await
    }

    #[inline]
    pub(crate) fn finish(mut self) -> Result<ComponentRegistry> {
        if !self.shelf.is_empty() {
            return Err(Error::InvalidState);
        }
        self.registry.take().ok_or(Error::InvalidState)
    }
}

impl Drop for RegistryBuilder {
    #[inline]
    fn drop(&mut self) {
        if let Some(registry) = self.registry.as_ref() {
            registry.shutdown_all();
        }
        // Provisions can retain quiescent guards into registered owners, so the
        // shelf must be drained before owner drop starts.
        self.shelf.clear();
    }
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
            access_map: HashMap::new(),
            boxed_vec: Vec::new(),
            shutdown_started: AtomicBool::new(false),
        }
    }

    /// Register one component owner and publish its dependency handle.
    ///
    /// Registration order is significant: later components may only depend on
    /// components that were registered earlier, and reverse registration order
    /// becomes the engine shutdown/drop order.
    #[inline]
    pub(crate) fn register<C: Component>(&mut self, owned: C::Owned) -> Result<()> {
        debug_assert!(
            !C::NAME.is_empty(),
            "component implementations must provide a stable non-empty name"
        );
        let tid = TypeId::of::<C>();
        if self.access_map.contains_key(&tid) {
            return Err(Error::EngineComponentAlreadyRegistered);
        }

        let owner = QuiescentBox::new(owned);
        let access = C::access(&owner);
        self.access_map.insert(tid, Box::new(access));
        self.boxed_vec
            .push(Box::new(TypedComponentBox::<C> { owner }));
        Ok(())
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
    #[inline]
    pub(crate) fn dependency<C: Component>(&self) -> Result<C::Access> {
        self.get::<C>()
            .ok_or(Error::EngineComponentMissingDependency)
    }

    /// Run explicit component shutdown in reverse registration order.
    ///
    /// This is idempotent at the registry level and is intended to stop worker
    /// activity before owner drop starts waiting on quiescent guards.
    #[inline]
    pub(crate) fn shutdown_all(&self) {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            return;
        }
        for component in self.boxed_vec.iter().rev() {
            component.shutdown();
        }
    }
}

impl Drop for ComponentRegistry {
    #[inline]
    fn drop(&mut self) {
        self.access_map.clear();
        while let Some(owner) = self.boxed_vec.pop() {
            drop(owner);
        }
    }
}

macro_rules! pool_access_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(Clone)]
        pub struct $name(QuiescentGuard<$inner>);

        impl $name {
            #[inline]
            pub fn clone_inner(&self) -> QuiescentGuard<$inner> {
                self.0.clone()
            }

            #[inline]
            pub fn into_inner(self) -> QuiescentGuard<$inner> {
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
pool_access_newtype!(IndexPool, FixedBufferPool);
pool_access_newtype!(MemPool, EvictableBufferPool);
pool_access_newtype!(DiskPool, GlobalReadonlyBufferPool);

#[derive(Clone, Copy)]
pub(crate) struct MetaPoolConfig {
    pub(crate) bytes: usize,
}

impl MetaPoolConfig {
    #[inline]
    pub(crate) fn new(bytes: usize) -> Self {
        Self { bytes }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct IndexPoolConfig {
    pub(crate) bytes: usize,
}

impl IndexPoolConfig {
    #[inline]
    pub(crate) fn new(bytes: usize) -> Self {
        Self { bytes }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct DiskPoolConfig {
    pub(crate) bytes: usize,
}

impl DiskPoolConfig {
    #[inline]
    pub(crate) fn new(bytes: usize) -> Self {
        Self { bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::sync::Arc;

    struct ValueComponent;

    impl Component for ValueComponent {
        type Config = ();
        type Owned = usize;
        type Access = usize;

        const NAME: &'static str = "value";

        #[inline]
        async fn build(
            _config: Self::Config,
            _registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> Result<()> {
            unreachable!("test-only component")
        }

        #[inline]
        fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access {
            **owner
        }

        #[inline]
        fn shutdown(_component: &Self::Owned) {}
    }

    struct ShutdownProbe {
        events: Arc<Mutex<Vec<&'static str>>>,
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

                const NAME: &'static str = $name;

                #[inline]
                async fn build(
                    _config: Self::Config,
                    _registry: &mut ComponentRegistry,
                    _shelf: ShelfScope<'_, Self>,
                ) -> Result<()> {
                    unreachable!("test-only component")
                }

                #[inline]
                fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

                #[inline]
                fn shutdown(component: &Self::Owned) {
                    component.events.lock().push($name);
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

        const NAME: &'static str = "access-order";

        #[inline]
        async fn build(
            _config: Self::Config,
            _registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> Result<()> {
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
    fn test_component_registry_rejects_duplicate_registration() {
        let mut registry = ComponentRegistry::new();
        registry.register::<ValueComponent>(7).unwrap();
        let err = registry.register::<ValueComponent>(11).unwrap_err();
        assert!(matches!(err, Error::EngineComponentAlreadyRegistered));
    }

    #[test]
    fn test_component_registry_reports_missing_dependency() {
        let registry = ComponentRegistry::new();
        let err = registry.dependency::<ValueComponent>().unwrap_err();
        assert!(matches!(err, Error::EngineComponentMissingDependency));
    }

    #[test]
    fn test_component_registry_returns_typed_access_clone() {
        let mut registry = ComponentRegistry::new();
        registry.register::<ValueComponent>(7).unwrap();
        assert_eq!(registry.get::<ValueComponent>(), Some(7));
    }

    #[test]
    fn test_component_registry_shutdown_uses_reverse_registration_order_and_is_idempotent() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ComponentRegistry::new();
        registry
            .register::<ShutdownA>(ShutdownProbe {
                events: Arc::clone(&events),
            })
            .unwrap();
        registry
            .register::<ShutdownB>(ShutdownProbe {
                events: Arc::clone(&events),
            })
            .unwrap();
        registry
            .register::<ShutdownC>(ShutdownProbe {
                events: Arc::clone(&events),
            })
            .unwrap();

        registry.shutdown_all();
        registry.shutdown_all();

        assert_eq!(events.lock().as_slice(), &["c", "b", "a"]);
    }

    #[test]
    fn test_component_registry_drop_clears_access_before_owner_drop() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut registry = ComponentRegistry::new();
        registry
            .register::<AccessOrderComponent>(OwnerProbe {
                events: Arc::clone(&events),
            })
            .unwrap();

        drop(registry);

        assert_eq!(events.lock().as_slice(), &["access", "owner"]);
    }

    struct Upstream;
    struct Downstream;

    impl Component for Upstream {
        type Config = ();
        type Owned = ();
        type Access = ();

        const NAME: &'static str = "up";

        async fn build(
            _config: Self::Config,
            _registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> Result<()> {
            unreachable!("test-only component")
        }

        fn access(_owner: &QuiescentBox<Self::Owned>) -> Self::Access {}

        fn shutdown(_component: &Self::Owned) {}
    }

    impl Component for Downstream {
        type Config = ();
        type Owned = ();
        type Access = ();

        const NAME: &'static str = "down";

        async fn build(
            _config: Self::Config,
            _registry: &mut ComponentRegistry,
            _shelf: ShelfScope<'_, Self>,
        ) -> Result<()> {
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
            up.put::<Downstream>(7).unwrap();
        }
        {
            let mut down = shelf.scope::<Downstream>();
            assert_eq!(down.take::<Upstream>(), Some(7));
            assert_eq!(down.take::<Upstream>(), None);
        }
        assert!(shelf.is_empty());
    }

    #[test]
    fn test_shelf_rejects_duplicate_edge_put() {
        let mut shelf = Shelf::new();
        let mut up = shelf.scope::<Upstream>();
        up.put::<Downstream>(7).unwrap();
        let err = up.put::<Downstream>(11).unwrap_err();
        assert!(matches!(err, Error::InvalidState));
    }
}
