use crate::buffer::{EvictableBufferPool, FixedBufferPool, GlobalReadonlyBufferPool};
use crate::error::{Error, Result};
use crate::quiescent::{QuiescentBox, QuiescentGuard};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) trait Component: Sized + 'static {
    type Config;
    type Owned: Send + Sync + 'static;
    type Access: Clone + Send + Sync + 'static;

    const NAME: &'static str;

    fn build(
        config: Self::Config,
        registry: &mut ComponentRegistry,
    ) -> impl Future<Output = Result<()>> + Send;

    fn access(owner: &QuiescentBox<Self::Owned>) -> Self::Access;

    fn shutdown(component: &Self::Owned);
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

pub(crate) struct ComponentRegistry {
    access_map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    boxed_vec: Vec<Box<dyn ErasedComponentBox>>,
    shutdown_started: AtomicBool,
}

impl Default for ComponentRegistry {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl ComponentRegistry {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            access_map: HashMap::new(),
            boxed_vec: Vec::new(),
            shutdown_started: AtomicBool::new(false),
        }
    }

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

    #[inline]
    pub(crate) fn get<C: Component>(&self) -> Option<C::Access> {
        self.access_map
            .get(&TypeId::of::<C>())
            .and_then(|v| v.downcast_ref::<C::Access>())
            .cloned()
    }

    #[inline]
    pub(crate) fn dependency<C: Component>(&self) -> Result<C::Access> {
        self.get::<C>()
            .ok_or(Error::EngineComponentMissingDependency)
    }

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
        async fn build(_config: Self::Config, _registry: &mut ComponentRegistry) -> Result<()> {
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
        async fn build(_config: Self::Config, _registry: &mut ComponentRegistry) -> Result<()> {
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
}
