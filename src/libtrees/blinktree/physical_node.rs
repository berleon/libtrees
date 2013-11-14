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
use utils;

pub trait PhysicalNode<K,V,Ptr> {
    fn new(node_types: uint, ptr: Ptr, link_ptr: Option<Ptr>,
           keys: ~[K], values: ~[V]) -> Self;

    // use this ptr to point to this node
    fn my_ptr<'a>(&'a self) -> &'a Ptr;

    /// returns the my_ptr of the next node to scan and true if we went of a link ptr
    fn link_ptr<'a>(&'a self) -> Option<&'a Ptr>;
    // returns the old link pointer
    fn set_link_ptr(&mut self, new_link_ptr: Ptr) -> Option<Ptr>;

    fn max_key<'a>(&'a self) -> &'a K;
    fn min_key<'a>(&'a self) -> &'a K;

    fn keys<'a>(&'a self) ->&'a ~[K];
    fn values<'a>(&'a self) -> &'a ~[V];
    fn mut_keys<'a>(&'a mut self) ->&'a mut ~[K];
    fn mut_values<'a>(&'a mut self) -> &'a mut ~[V];


    fn is_root(&self) -> bool;
    fn set_root(&mut self);
    fn unset_root(&mut self);
    fn is_leaf(&self) -> bool;
    fn is_inode(&self) -> bool;

    fn is_most_right_node(&self) -> bool {
        self.link_ptr().is_none()
    }

    fn needs_split(&self, max_size: uint) -> bool;
    fn split_at(&mut self, position: uint) -> (~[K],~[V]);

}

pub static T_ROOT: uint = 1 >> 0;
pub static T_LEAF: uint = 1 >> 1;
pub static T_INODE: uint = 1 >> 2;

#[deriving(Clone)]
pub struct DefaultBLinkNode<K, V, Ptr> {
    node_type: uint,
    my_ptr: Ptr,
    keys: ~[K],
    values: ~[V],
    link_ptr: Option<Ptr>
}
fn is_node_type(tpe: uint, node_type: uint) -> bool {
    tpe & node_type == node_type
}
#[test]
fn test_is_node_type() {
    assert!(is_node_type(T_ROOT, T_ROOT));
    assert!(is_node_type(T_ROOT, T_ROOT));
    assert!(! is_node_type(T_LEAF, T_ROOT));
    assert!(! is_node_type(0, T_ROOT));
}

fn set_node_type(tpe: &mut uint, node_type: uint) {
    *tpe =  *tpe | node_type;
}
#[test]
fn test_set_node_type() {
    let tpe = &mut 0;
    set_node_type(tpe, T_ROOT);
    assert!(*tpe == T_ROOT);
}
impl <K,V,Ptr>
PhysicalNode<K,V,Ptr> for DefaultBLinkNode<K,V,Ptr> {
    fn new(node_type: uint, ptr: Ptr, link_ptr: Option<Ptr>,
           keys: ~[K], values: ~[V]) -> DefaultBLinkNode<K, V, Ptr> {
        DefaultBLinkNode {
            node_type: node_type,
            my_ptr: ptr,
            keys: keys,
            values: values,
            link_ptr: link_ptr
        }
    }
    fn my_ptr<'a>(&'a self) -> &'a Ptr {
        return &self.my_ptr;
    }
    fn link_ptr<'a>(&'a self) -> Option<&'a Ptr> {
        match self.link_ptr {
            Some(ref r) => Some(r),
            None => None
        }
    }
    fn set_link_ptr(&mut self, new_link_ptr: Ptr) -> Option<Ptr> {
        let old_link_ptr = self.link_ptr.take();
        self.link_ptr = Some(new_link_ptr);
        return old_link_ptr;
    }
    fn max_key<'a>(&'a self) -> &'a K {
        &self.keys[self.keys.len()-1]
    }
    fn min_key<'a>(&'a self) -> &'a K {
        &self.keys[0]
    }
    fn keys<'a>(&'a self) -> &'a ~[K] {
        &self.keys
    }
    fn values<'a>(&'a self) -> &'a ~[V] {
        &self.values
    }
    fn mut_keys<'a>(&'a mut self) ->&'a mut ~[K] {
        &mut self.keys
    }
    fn mut_values<'a>(&'a mut self) -> &'a mut ~[V] {
        &mut self.values
    }
    fn is_root(&self) -> bool {
        is_node_type(self.node_type,T_ROOT)
    }
    fn set_root(&mut self) {
        set_node_type(&mut self.node_type, T_ROOT);
    }
    fn unset_root(&mut self) {
        self.node_type = self.node_type & (! T_ROOT);
    }
    fn is_inode(&self) -> bool {
        is_node_type(self.node_type,T_INODE)
    }
    fn is_leaf(&self) -> bool {
        is_node_type(self.node_type,T_LEAF)
    }
    fn set_root(&mut self) {
        set_node_type(&mut self.node_type, T_ROOT);
    }

    fn needs_split(&self, max_size: uint) -> bool {
        max_size < self.keys.len()
    }
    fn split_at(&mut self, position: uint) -> (~[K],~[V]) {
        let ret_keys = utils::split_at(&mut self.keys, position);
        let ret_values = utils::split_at(&mut self.values, position);
        (ret_keys, ret_values)
    }
}
