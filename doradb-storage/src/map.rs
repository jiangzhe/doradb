use std::collections::{HashMap, HashSet};

/// Fast non-cryptographic hash builder used by internal hash collections.
pub(crate) type FastRandomState = ahash::RandomState;
/// Hash map using the engine's fast hash builder.
pub(crate) type FastHashMap<K, V> = HashMap<K, V, FastRandomState>;
/// Hash set using the engine's fast hash builder.
pub(crate) type FastHashSet<T> = HashSet<T, FastRandomState>;
/// DashMap using the engine's fast hash builder.
pub(crate) type FastDashMap<K, V> = dashmap::DashMap<K, V, FastRandomState>;
