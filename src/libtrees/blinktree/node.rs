/* Copyright 2013 Leon Sixt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use algorithm;
use utils;
use blinktree::blinktree::{is_in_state, ROOT};

pub enum Movement {
    Right,
    Down,
    MostLeftDown,
    MostRightDown
}


/// Some operations on a node require knowledge about the position of the node in the tree.
/// For example if the current node is the root node, it can contain every posible key.
pub trait BLinkNode<K, V, Ptr> {
    fn new(ptr: Ptr, right: Option<Ptr>, keys: ~[K], values: ~[V]) -> Self;

    // use this ptr to point to this node
    fn my_ptr<'a>(&'a self) -> &'a Ptr;
    /// returns the my_ptr of the next node to scan and true if we went of a link ptr
    fn link_ptr<'a>(&'a self) -> Option<&'a Ptr>;


    fn max_key<'a>(&'a self) -> &'a K;
    fn min_key<'a>(&'a self) -> &'a K;


    fn is_root(&self, state: u8) -> bool;
    fn is_leaf(&self) -> bool;
    fn is_inode(&self) -> bool;
    fn is_most_right_node(&self) -> bool {
        self.link_ptr().is_none()
    }

    fn move_right<'a>(&'a self, state: u8, key: &K) -> Option<&'a Ptr>;

    fn get<'a>(&'a self, state: u8, key: &K) -> Option<&'a V>;
    fn scannode<'a>(&'a self, state: u8, key: &K) -> Option<(&'a Ptr, Movement)>;
    fn needs_split(&self, max_size: uint) -> bool;
    fn split_and_insert(&mut self, state: u8, new_page: Ptr, key: K, value: V) -> ~Self;
    fn insert(&mut self, state: u8, key: K, value: V);
    fn can_contain_key(&self, state: u8, key: &K) -> bool;
}

#[deriving(Clone)]
pub struct SimpleBLinkRawNode<K, V, Ptr> {
    my_ptr: Ptr,
    keys: ~[K],
    values: ~[V],
    right: Option<Ptr>
}


#[deriving(Clone)]
pub struct SimpleBLinkINode<K, V, Ptr> {
    inode: SimpleBLinkRawNode<K, V, Ptr>
}

impl <K: TotalOrd + ToStr, Ptr: Clone + ToStr>
BLinkNode<K, Ptr, Ptr>
for SimpleBLinkINode<K, Ptr, Ptr> {

    fn new(ptr: Ptr, right: Option<Ptr>, keys: ~[K], values: ~[Ptr]) -> SimpleBLinkINode<K, Ptr, Ptr> {
        SimpleBLinkINode {
            inode: SimpleBLinkRawNode::new(ptr, right, keys, values)
        }
    }
    fn my_ptr<'a>(&'a self) -> &'a Ptr {
        return &self.inode.my_ptr;
    }
    fn link_ptr<'a>(&'a self) -> Option<&'a Ptr> {
        match self.inode.right {
            Some(ref r) => Some(r),
            None => None
        }
    }
    fn max_key<'a>(&'a self) -> &'a K {
        &self.inode.keys[self.inode.keys.len()-1]
    }
    fn min_key<'a>(&'a self) -> &'a K {
        &self.inode.keys[0]
    }

    fn is_root(&self, state: u8) -> bool {
        is_in_state(state, ROOT)
    }
    fn is_inode(&self) -> bool { true }
    fn is_leaf(&self)  -> bool { false }

    fn scannode<'a>(&'a self, state: u8, key: &K) -> Option<(&'a Ptr, Movement)> {
        if(! self.can_contain_key(state, key)) {
            return self.inode.right.as_ref().map(|r| (r, Right));
        }
        self.get(state, key).map(|r| (r, Down))
    }


    fn move_right<'a>(&'a self, state: u8, key: &K) -> Option<&'a Ptr> {
        if(! self.can_contain_key(state, key)) {
            self.inode.right.as_ref()
        } else {
            None
        }
    }

    fn can_contain_key(&self, state: u8, key: &K) -> bool {
        self.inode.can_contain_key(state, key)
    }

    fn get<'a>(&'a self, state: u8, key: &K) -> Option<&'a Ptr> {
        if !self.can_contain_key(state, key) {
            return None;
        }
        let idx = algorithm::bsearch_idx(self.inode.keys, key);
        debug!("[SimpleBLinkINode.get] ptr: {}, keys: {} values: {}, key: {}, idx: {}",
                 self.my_ptr().to_str(), self.inode.keys.to_str(),
                 self.inode.values.to_str(), key.to_str(), idx.to_str());
        Some(&self.inode.values[idx])
    }

    fn needs_split(&self, max_size: uint) -> bool {
        max_size < self.inode.keys.len()
    }
    fn split_and_insert(&mut self, state: u8, new_page_ptr: Ptr, key: K, value: Ptr)
        -> ~SimpleBLinkINode<K, Ptr, Ptr> {
        let new_size = self.inode.values.len()/2;
        self.insert(state, key, value);
        let values_new = utils::split_at(&mut self.inode.values, new_size);
        let keys_new = utils::split_at(&mut self.inode.keys, new_size);
        let right = self.inode.right.take();
        self.inode.right = Some(new_page_ptr.clone());
        ~BLinkNode::new(new_page_ptr, right, keys_new, values_new)

    }

    fn insert(&mut self, state: u8, key: K, value: Ptr) {
        let mut idx = algorithm::bsearch_idx(self.inode.keys, &key);
        self.inode.keys.insert(idx, key);

        if is_in_state(state, ROOT) || self.is_most_right_node() {
            idx += 1;
        }
        self.inode.values.insert(idx, value);
    }
}
#[deriving(Clone)]
pub struct SimpleBLinkLeaf<K, V, Ptr> {
    leaf: SimpleBLinkRawNode<K, V, Ptr>
}

impl <K: TotalOrd + ToStr,
      V: ToStr,
      Ptr: Clone + ToStr>
BLinkNode<K,V, Ptr>
for SimpleBLinkLeaf<K, V, Ptr> {

    fn new(ptr: Ptr, right: Option<Ptr>, keys: ~[K], values: ~[V]) -> SimpleBLinkLeaf<K, V, Ptr> {
        SimpleBLinkLeaf {
            leaf: SimpleBLinkRawNode::new(ptr, right, keys, values)
        }
    }

    fn my_ptr<'a>(&'a self) -> &'a Ptr {
        return &self.leaf.my_ptr;
    }
    fn link_ptr<'a>(&'a self) -> Option<&'a Ptr> {
        match self.leaf.right {
            Some(ref r) => Some(r),
            None => None
        }
    }
    fn max_key<'a>(&'a self) -> &'a K {
        &self.leaf.keys[self.leaf.keys.len()-1]
    }
    fn min_key<'a>(&'a self) -> &'a K {
        &self.leaf.keys[0]
    }

    fn is_root(&self, state: u8) -> bool {
        is_in_state(state, ROOT)
    }
    fn is_inode(&self) -> bool { false }
    fn is_leaf(&self)  -> bool { true }

    fn scannode<'a>(&'a self, state: u8, key: &K) -> Option<(&'a Ptr, Movement)> {
        if(! self.can_contain_key(state, key)) {
            self.leaf.link_ptr().map(|r| (r, Right))
        } else {
            None
        }
    }
    fn move_right<'a>(&'a self, state: u8, key: &K) -> Option<&'a Ptr> {
        if(! self.can_contain_key(state, key)) {
            self.leaf.link_ptr()
        } else {
            None
        }
    }
    fn get<'a>(&'a self, state: u8, key: &K) -> Option<&'a V> {
        if !self.can_contain_key(state, key) || self.leaf.keys.is_empty() {
            return None;
        }
        let idx = algorithm::bsearch_idx(self.leaf.keys, key);

        debug!("[SimpleBLinkLeaf.get] ptr: {}, keys: {} values: {}, key: {}, idx: {}",
                 self.my_ptr().to_str(), self.leaf.keys.to_str(), self.leaf.values.to_str(), key.to_str(), idx.to_str());

        if self.leaf.keys[idx].cmp(key) == Equal {
            Some(&self.leaf.values[idx])
        } else {
            None
        }
    }
    fn needs_split(&self, max_size: uint) -> bool {
        max_size < self.leaf.keys.len()
    }
    fn split_and_insert(&mut self, state: u8, new_page_ptr: Ptr, key: K, value: V) -> ~SimpleBLinkLeaf<K, V, Ptr> {
        let new_size = self.leaf.values.len()/2;
        self.insert(state, key, value);
        let values_new = utils::split_at(&mut self.leaf.values, new_size);
        let keys_new = utils::split_at(&mut self.leaf.keys, new_size);
        let right = self.leaf.right.take();
        self.leaf.right = Some(new_page_ptr.clone());
        ~BLinkNode::new(new_page_ptr, right, keys_new, values_new)
    }

    fn can_contain_key(&self, state: u8, key: &K) -> bool {
        self.leaf.can_contain_key(state, key)
    }

    #[allow(unused_variable)]
    fn insert(&mut self, state: u8, key: K, value: V) {
        let idx = algorithm::bsearch_idx(self.leaf.keys, &key);
        self.leaf.values.insert(idx, value);
        self.leaf.keys.insert(idx, key);
    }
}

impl<'self,
    K: TotalOrd,
    V,
    Ptr>
SimpleBLinkRawNode<K, V, Ptr> {
    fn new(ptr: Ptr, right: Option<Ptr>, keys: ~[K], values: ~[V]) -> SimpleBLinkRawNode<K,V,Ptr> {
        SimpleBLinkRawNode {
            my_ptr: ptr,
            keys: keys,
            values: values,
            right: right
        }
    }
    fn my_ptr<'a>(&'a self) -> &'a Ptr {
        return &self.my_ptr;
    }

    fn link_ptr<'a>(&'a self) -> Option<&'a Ptr> {
        match self.right {
            Some(ref r) => Some(r),
            None => None
        }
    }

    fn max_key<'a>(&'a self) -> &'a K {
        &self.keys[self.keys.len()-1]
    }
    fn min_key<'a>(&'a self) -> &'a K {
        &self.keys[0]
    }
    fn size(&self) -> uint { self.keys.len() }

    fn can_contain_key(&self, state: u8, key: &K) -> bool {
           is_in_state(state, ROOT)
        || (self.right.is_none() && key.cmp(self.max_key()) == Greater)
        || (key.cmp(self.max_key()) == Less ||
            key.cmp(self.max_key()) == Equal)
    }
}
#[cfg(test)]
mod test {
    use super::{BLinkNode, SimpleBLinkRawNode, SimpleBLinkINode};
    use blinktree::blinktree::{ROOT};
    macro_rules! can_contains_range(
        ($name:ident, $state:ident, $from:expr, $to:expr) => (
            for i in range($from, $to+1) {
                assert!($name.can_contain_key($state, &i), format!("cannot contain key {}, state is {:t}", i, $state));
            }
        )
    )
    #[test]
    fn test_can_contain_key() {
        let leaf = SimpleBLinkRawNode::new(0u, None, ~[2u],~[0,1]);
        let state = ROOT;
        can_contains_range!(leaf, state, 0u, 10);
        assert!(leaf.can_contain_key(state, &10000));

        let state = 0;
        let leaf = SimpleBLinkRawNode::new(0u, None, ~[2u,4],~[0,1]);

        can_contains_range!(leaf, state, 0u, 4);
    }
    #[test]
    fn test_needs_split() {
        let leaf = SimpleBLinkRawNode::new(0u, None, ~[2u],~[0,1]);
        let state = ROOT;
        can_contains_range!(leaf, state, 0u, 10);
        assert!(leaf.can_contain_key(state, &10000));

        let state = 0;
        let leaf = SimpleBLinkRawNode::new(0u, None, ~[2u,4],~[0,1]);

        can_contains_range!(leaf, state, 0u, 4);
    }
    //           root                  otherwise
    //  keys:    . 4 .                  1 | 2 | 3
    //  values:  1   3                 10   1   4
    #[test]
    fn test_root_insert_value_must_be_off_by_one() {
        let mut inode: SimpleBLinkINode<uint,uint,uint> = BLinkNode::new(0u, None, ~[1],~[0,1]);
        let state = ROOT;
        inode.insert(state, 4, 4);
        inode.insert(state, 3, 3);
        let expected = ~[0,1,3,4];
        assert!(inode.inode.values == expected,
                format!("expected: {}, got {}", expected.to_str(), inode.inode.values.to_str()))
    }

}
