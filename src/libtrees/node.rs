use algorithm;
use std::to_bytes::IterBytes;

pub trait Node<K, V> {
    fn scannode<'a>(&'a self, key: &K) -> Option<(uint, bool)>;
    fn get<'a>(&'a self, key: &K) -> Option<&'a V>;
    fn isLeaf(&self) -> bool;
    fn can_contains(&self, key: &K) -> bool;
}
pub enum BlinkTreeNodes<N, V> {
    InnerNodes(N),
    Leafs(V)
}

struct SimpleBlinkTreeNode<'self, K,V> {
    is_leaf: bool,
    keys: &'self [K],
    values: BlinkTreeNodes<&'self [uint], &'self [V]>,
    right: Option<uint>
}

impl<'self,
    K: TotalOrd,
    V> Node<K,V>
for SimpleBlinkTreeNode<'self, K, V> {
    fn scannode<'a>(&'a self, key: &K) -> Option<(uint, bool)> {
        if(! self.can_contains(key)) {
            return self.right.map(|r| (r, true));
        }

        if self.isLeaf() { return None; }

        let idx = algorithm::bsearch_idx(self.keys, key);
        match self.values {
            InnerNodes(ids) => Some((ids[idx], false)),
            _ => None,
        }
    }
    fn get<'a>(&'a self, key: &K) -> Option<&'a V> {
        if !self.isLeaf() { return None; }
        let idx = algorithm::bsearch_idx(self.keys, key);
        match self.values {
            Leafs(ids) => Some(&ids[idx]),
            _ => None,
        }

    }
    fn isLeaf(&self) -> bool {
        self.is_leaf
    }
    fn can_contains(&self, key: &K) -> bool {
        ( self.keys[0].cmp(key) == Greater ||
          self.keys[0].cmp(key) == Equal )&&
        ( key.cmp(&self.keys[self.keys.len()-1]) == Less||
          key.cmp(&self.keys[self.keys.len()-1]) == Equal )
    }

}
