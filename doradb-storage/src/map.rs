pub(crate) type FastRandomState = ahash::RandomState;
pub(crate) type FastHashMap<K, V> = std::collections::HashMap<K, V, FastRandomState>;
pub(crate) type FastHashSet<T> = std::collections::HashSet<T, FastRandomState>;
pub(crate) type FastDashMap<K, V> = dashmap::DashMap<K, V, FastRandomState>;
