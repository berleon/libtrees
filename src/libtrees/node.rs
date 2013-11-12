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

use blinktree::physical_node::{PhysicalNode, NodeTypes};

#[deriving(Clone)]
pub enum Node<I, L> {
    INode(I),
    Leaf(L)
}

impl <I,L> Node<I,L> {
    pub fn isLeaf(&self) -> bool {
        match self{
            &Leaf(*)  => true,
            &INode(*) => false
        }
    }
    pub fn isINode(&self) -> bool {
        match self{
            &INode(*) => true,
            &Leaf(*)  => false
        }
    }
    pub fn getLeaf<'a>(&'a self) -> &'a L {
        match self{
            &Leaf(ref l)  => l,
            &INode(*) => fail!("called getLeaf on an INode"),
        }
    }
    pub fn getINode<'a>(&'a self) -> &'a I {
        match self {
            &INode(ref i) => i,
            &Leaf(*)  => fail!("called getINode on a Leaf"),
        }
    }
}

macro_rules! node_method(
    ($method:ident) => (
        match self {
            &INode(ref inode) => inode.$method(),
            &Leaf(ref leaf) => leaf.$method()
        }
    )
)

impl <K,V,Ptr,
     INODE: PhysicalNode<K,Ptr,Ptr>,
     LEAF: PhysicalNode<K,V,Ptr>>
Node<INODE, LEAF> {
    pub fn my_ptr<'a>(&'a self) -> &'a Ptr {
        node_method!(my_ptr)
    }
    /// returns the my_ptr of the next node to scan and true if we went of a link ptr
    pub fn link_ptr<'a>(&'a self) -> Option<&'a Ptr> {
        node_method!(link_ptr)
    }

    pub fn max_key<'a>(&'a self) -> &'a K {
        node_method!(max_key)
    }
    pub fn min_key<'a>(&'a self) -> &'a K {
        node_method!(min_key)
    }
    pub fn keys<'a>(&'a self) -> &'a ~[K] {
        node_method!(keys)
    }

    pub fn is_root(&self) -> bool {
        node_method!(is_root)
    }
    pub fn is_leaf(&self) -> bool {
        node_method!(is_leaf)
    }
    pub fn is_inode(&self) -> bool {
        node_method!(is_inode)
    }
    pub fn add_type(&mut self, node_type: NodeTypes) {
        match self {
            &INode(ref mut inode) => inode.add_type(node_type),
            &Leaf(ref mut leaf) => leaf.add_type(node_type)
        }
    }

    pub fn is_most_right_node(&self) -> bool {
        self.link_ptr().is_none()
    }

    pub fn needs_split(&self, max_size: uint) -> bool {
        match self {
            &INode(ref inode) => inode.needs_split(max_size),
            &Leaf(ref leaf) => leaf.needs_split(max_size)
        }
    }
}
