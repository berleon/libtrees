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
use extra::enum_set::{EnumSet, CLike};

use utils;

pub trait PhysicalNode<K,V,Ptr> {
    fn new(node_types: EnumSet<NodeTypes>, ptr: Ptr, link_ptr: Option<Ptr>,
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
    fn is_leaf(&self) -> bool;
    fn is_inode(&self) -> bool;
    fn add_type(&mut self, tpe: NodeTypes);
    fn is_most_right_node(&self) -> bool {
        self.link_ptr().is_none()
    }

    fn needs_split(&self, max_size: uint) -> bool;
    fn split_at(&mut self, position: uint) -> (~[K],~[V]);

}

pub fn inode_type() -> EnumSet<NodeTypes> {
    let mut set = EnumSet::empty();
    set.add(INode);
    set
}

pub fn leaf_type() -> EnumSet<NodeTypes> {
    let mut set = EnumSet::empty();
    set.add(Leaf);
    set
}
pub fn root_type() -> EnumSet<NodeTypes> {
    let mut set = EnumSet::empty();
    set.add(Root);
    set
}

#[deriving(Clone)]
pub enum NodeTypes {
    Root,
    Leaf,
    INode
}
impl CLike for NodeTypes {
    fn to_uint(&self) -> uint {
        match *self {
            Root  => 1 << 0,
            Leaf  => 1 << 1,
            INode => 1 << 2,
        }
    }
    fn from_uint(number: uint) -> NodeTypes {
        match number {
            1 =>  Root,
            2 =>  Leaf,
            4 =>  INode,
            _ => fail!()
        }
    }
}
#[deriving(Clone)]
pub struct DefaultBLinkNode<K, V, Ptr> {
    node_type: EnumSet<NodeTypes>,
    my_ptr: Ptr,
    keys: ~[K],
    values: ~[V],
    link_ptr: Option<Ptr>
}

impl <K,V,Ptr>
PhysicalNode<K,V,Ptr> for DefaultBLinkNode<K,V,Ptr> {
    fn new(node_type: EnumSet<NodeTypes>, ptr: Ptr, link_ptr: Option<Ptr>,
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
        self.node_type.contains_elem(Root)
    }
    fn is_inode(&self) -> bool {
        self.node_type.contains_elem(INode)
    }
    fn add_type(&mut self, tpe: NodeTypes) {
        self.node_type.add(tpe);
    }
    fn is_leaf(&self)  -> bool {
        self.node_type.contains_elem(INode)
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
