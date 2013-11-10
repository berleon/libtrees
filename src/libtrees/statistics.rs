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


use std::unstable::atomics::{AtomicUint, Relaxed};
use std::cast;

pub trait StatisticsManager: Freeze {
    fn elements(&self) -> uint;
    fn inc_elements(&self);
    fn dec_elements(&self);

    fn inodes(&self) -> uint;
    fn inc_inodes(&self);
    fn dec_inodes(&self);

    fn leafs(&self) -> uint;
    fn inc_leafs(&self);
    fn dec_leafs(&self);

    fn deletions(&self) -> uint;
    fn inc_deletions(&self);

    fn insertions(&self) -> uint;
    fn inc_insertions(&self);
}
pub struct AtomicStatistics {
    elements: AtomicUint,
    inodes: AtomicUint,
    leafs: AtomicUint,
    insertions: AtomicUint,
    deletions: AtomicUint,

}

impl StatisticsManager for AtomicStatistics {
    fn elements(&self) -> uint {
        self.elements.load(Relaxed)
    }
    fn inc_elements(&self){
        unsafe {
            cast::transmute_mut(&self.elements).fetch_add(1, Relaxed);
        }
    }
    fn dec_elements(&self){
        unsafe {
            cast::transmute_mut(&self.elements).fetch_sub(1, Relaxed);
        }
    }
    fn insertions(&self) -> uint {
        self.insertions.load(Relaxed)
    }
    fn inc_insertions(&self) {
        unsafe {
            cast::transmute_mut(&self.insertions).fetch_add(1, Relaxed);
        }
    }
    fn deletions(&self) -> uint {
        self.deletions.load(Relaxed)
    }
    fn inc_deletions(&self) {
        unsafe {
            cast::transmute_mut(&self.deletions).fetch_add(1, Relaxed);
        }
    }
    fn inodes(&self) -> uint {
        self.inodes.load(Relaxed)
    }
    fn inc_inodes(&self) {
        unsafe {
            cast::transmute_mut(&self.inodes).fetch_add(1, Relaxed);
        }
    }
    fn dec_inodes(&self){
        unsafe {
            cast::transmute_mut(&self.inodes).fetch_sub(1, Relaxed);
        }
    }

    fn leafs(&self) -> uint {
        self.leafs.load(Relaxed)
    }
    fn inc_leafs(&self) {
        unsafe {
            cast::transmute_mut(&self.leafs).fetch_add(1, Relaxed);
        }
    }
    fn dec_leafs(&self){
        unsafe {
            cast::transmute_mut(&self.leafs).fetch_sub(1, Relaxed);
        }
    }
}

impl AtomicStatistics {
    pub fn new() -> AtomicStatistics {
        AtomicStatistics {
            elements: AtomicUint::new(0),
            inodes: AtomicUint::new(0),
            leafs: AtomicUint::new(0),
            insertions: AtomicUint::new(0),
            deletions: AtomicUint::new(0)
        }

    }
}
