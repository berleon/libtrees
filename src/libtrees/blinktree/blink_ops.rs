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
use node::{Node, Leaf, INode};
use blinktree::physical_node::{PhysicalNode, T_LEAF, T_INODE};

#[deriving(Clone)]
pub enum Movement {
    Right,
    Down,
}


pub trait BLinkOps<K: TotalOrd + ToStr,
                   V: ToStr,
                   Ptr: Clone + ToStr,
                   INODE: PhysicalNode<K,Ptr,Ptr>,
                   LEAF: PhysicalNode<K,V,Ptr>> {
    fn move_right<'a>(&self, node: &'a Node<INODE,LEAF>, key: &K) -> Option<&'a Ptr> {
        let can_contain = match node {
            &Leaf(ref leaf) => self.can_contain_key(leaf, key),
            &INode(ref inode) => self.can_contain_key(inode, key)
        };
        if ! can_contain {
            node.link_ptr()
        } else {
            None
        }
    }
    fn get_value<'a>(&self, leaf: &'a LEAF, key: &K) -> Option<&'a V> {
        if !self.can_contain_key(leaf,key) {
            return None;
        }
        let idx = algorithm::bsearch_idx(leaf.keys().slice_from(0), key);
        debug!("[get] ptr: {}, keys: {} values: {}, key: {}, idx: {}",
                 leaf.my_ptr().to_str(), leaf.keys().to_str(),
                 leaf.values().to_str(), key.to_str(), idx.to_str());
        if leaf.keys()[idx].cmp(key) == Equal {
            Some(&leaf.values()[idx])
        } else {
            None
        }
    }
    fn get_ptr<'a>(&self, inode: &'a INODE, key: &K) -> Option<&'a Ptr> {
        if !self.can_contain_key(inode,key) {
            return None;
        }
        let idx = algorithm::bsearch_idx(inode.keys().slice_from(0), key);

        debug!("[get_ptr] key: {}, ptr: {}, keys: {} values: {}, idx: {}, is_most_right_node: {}, is_root: {}",
                 key.to_str(), inode.my_ptr().to_str(), inode.keys().to_str(),
                 inode.values().to_str(), idx.to_str(), inode.is_most_right_node(), inode.is_root());
        Some(&inode.values()[idx])
    }

    fn scannode<'a>(&self, node: &'a Node<INODE,LEAF>, key: &K) -> Option<(&'a Ptr, Movement)> {
        let can_contain = match node {
            &Leaf(ref leaf) => self.can_contain_key(leaf, key),
            &INode(ref inode) => self.can_contain_key(inode, key)
        };
        if(! can_contain) {
            return node.link_ptr().map(|r| (r, Right));
        }
        match node {
            &Leaf(*) => None,
            &INode(ref inode) =>
                self.get_ptr(inode, key).map(|r| (r, Down))
        }
    }

    fn split_and_insert_leaf(&self, leaf: &mut LEAF, new_page: Ptr, key: K, value: V) -> LEAF {
        let new_size = leaf.keys().len()/2;
        self.insert_leaf(leaf, key, value);
        let (keys_new, values_new) = leaf.split_at(new_size);
        let link_ptr = leaf.set_link_ptr(new_page.clone());
        PhysicalNode::new(T_LEAF, new_page, link_ptr, keys_new, values_new)
    }

    /// Default splitting strategy:
    ///
    /// example max_size = 4:
    ///                                split
    ///                                  |
    ///    |<= 3 <|<= 5 <|<= 10 <|<= 15 <|<= 30
    ///    .      .      .       .       .
    ///
    fn split_and_insert_inode(&self, inode: &mut INODE, new_page: Ptr, key: K, value: Ptr) -> INODE {
        let new_size = inode.keys().len()/2;
        self.insert_inode(inode, key, value);
        let (keys_new, values_new) = inode.split_at(new_size);
        debug!("[split_and_insert_inode] keys.len: {}, value.len: {}", keys_new.to_str(), values_new.to_str());
        let link_ptr = inode.set_link_ptr(new_page.clone());
        PhysicalNode::new(T_INODE, new_page, link_ptr, keys_new, values_new)
    }

    fn insert_leaf(&self, leaf: &mut LEAF, key: K, value: V) {
        let idx = algorithm::bsearch_idx(leaf.keys().slice_from(0), &key);
        leaf.mut_keys().insert(idx, key);
        leaf.mut_values().insert(idx, value);
    }
    fn insert_inode(&self, inode: &mut INODE, key: K, value: Ptr) {
        let mut idx = algorithm::bsearch_idx(inode.keys().slice_from(0), &key);
        inode.mut_keys().insert(idx, key);

        //if (inode.is_root() || inode.is_most_right_node()) {
            idx += 1;
        //}
        inode.mut_values().insert(idx, value);
    }
    fn can_contain_key<
        K1: TotalOrd,
        V1,
        Ptr1,
        N : PhysicalNode<K1,V1,Ptr1>>(&self, node: &N, key: &K1) -> bool {
        node.is_root()
        || (node.is_most_right_node() && key.cmp(node.max_key()) == Greater)
        || (key.cmp(node.max_key()) == Less ||
            key.cmp(node.max_key()) == Equal)
    }
}

pub struct DefaultBLinkOps<K,V,Ptr, INODE, LEAF>;

impl <K: TotalOrd + ToStr,
      V: ToStr,
      Ptr: Clone + ToStr,
      INODE: PhysicalNode<K,Ptr,Ptr>,
      LEAF: PhysicalNode<K,V,Ptr>
      >
BLinkOps<K,V,Ptr,INODE, LEAF> for DefaultBLinkOps<K,V,Ptr, INODE, LEAF> {}

#[cfg(test)]
mod test {
    use super::{BLinkOps, DefaultBLinkOps};
    use blinktree::physical_node::{PhysicalNode, DefaultBLinkNode, T_ROOT, T_LEAF};
    macro_rules! can_contains_range(
        ($node:ident, $from:expr, $to:expr) => (
            for i in range($from, $to+1) {
                assert!(self.can_contain_key(&$node, &i),
                    format!("cannot contain key {}, is_root: {}, is_leaf: {}, is_inode: {}",
                            i, $node.is_root(), $node.is_leaf(), $node.is_inode()));
            }
        )
    )
    trait BLinkOpsTest<INODE: PhysicalNode<uint, uint, uint>,
                       LEAF: PhysicalNode<uint, uint, uint>>
        : BLinkOps<uint,uint,uint,INODE,LEAF> {
        fn test(&self) {
            self.test_can_contain_key();
            self.test_needs_split();
            self.test_insert_into_inode_ptr_must_be_off_by_one();
        }
        fn test_can_contain_key(&self) {
            let tpe = T_ROOT ^ T_LEAF;
            let root : DefaultBLinkNode<uint, uint, uint> =
                PhysicalNode::new(tpe, 0u, None, ~[2u],~[0u,1u]);
            can_contains_range!(root, 0u, 10);
            assert!(self.can_contain_key(&root, &10000));

            let leaf : DefaultBLinkNode<uint, uint, uint> =
                PhysicalNode::new(T_LEAF, 0u, None, ~[2u,4],~[0,1]);
            can_contains_range!(leaf, 0u, 4);
        }
        fn test_needs_split(&self) {
        }

        //           ionde                otherwise
        //  keys:    . 4 .                  1 | 2 | 3
        //  values:  1   3                 10   1   4
        fn test_insert_into_inode_ptr_must_be_off_by_one(&self) {
            let mut inode: INODE = PhysicalNode::new(T_ROOT & T_LEAF, 0u, None, ~[1],~[0,1]);
            self.insert_inode(&mut inode, 4, 4);
            self.insert_inode(&mut inode, 3, 3);
            let expected = ~[0,1,3,4];
            assert!(inode.values() == &expected,
                    format!("expected: {}, got {}", expected.to_str(), inode.values().to_str()))
        }
    }
    impl BLinkOpsTest<DefaultBLinkNode<uint, uint, uint>,
                      DefaultBLinkNode<uint, uint, uint>>
    for DefaultBLinkOps<uint,uint,uint,
                           DefaultBLinkNode<uint,uint,uint>,
                           DefaultBLinkNode<uint, uint, uint>> {}


    #[test]
    fn test_default_blink_ops() {
        let ops = DefaultBLinkOps;
        ops.test();
    }
}
