
pub trait Map<K,V> {
    fn find<'a>(&'a self, key: &K) -> Option<&'a V>;
    fn contains_key(&self, key: &K) -> bool {
        self.find(key).is_some()
    }
    fn insert(&self, key: K, value: V);
    fn remove(&self, key: &K);
}

trait IteratableMap<'self, K,V, I: Iterator<(K,V)>> {
    fn iter(from: &K, to: &K) -> I;
    fn iter_all() -> I;
}
