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

