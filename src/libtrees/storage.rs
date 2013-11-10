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


use std::cast;
use std::hash::Hash;
use std::hashmap::HashMap;

use std::unstable::atomics::{AtomicUint, Relaxed};

pub trait StorageManager<Ptr, N>: Freeze {
    fn new_page(&self) -> Ptr;
    fn read<'a>(&'a self, id: &Ptr) -> Option<&'a N>;
    fn write(&mut self, id: &Ptr, node: &N);
}

pub struct StupidHashmapStorage<Ptr, N> {
    last_page_ptr: AtomicUint,
    map: HashMap<Ptr, N>,
}

impl<Ptr: Eq + Hash + Freeze,
     N: Freeze>
StupidHashmapStorage<Ptr, N>  {
    pub fn new() -> StupidHashmapStorage<Ptr, N> {
        StupidHashmapStorage {
            last_page_ptr: AtomicUint::new(1),
            map: HashMap::new()
        }
    }
}

impl<N: Freeze + Clone> StorageManager<uint, N> for StupidHashmapStorage<uint, N> {
    fn new_page(&self) -> uint {
        unsafe {
            cast::transmute_mut(self).last_page_ptr.fetch_add(1, Relaxed)
        }
    }
    fn read<'a>(&'a self, id: &uint) -> Option<&'a N> {
        self.map.find(id)
    }
    fn write(&mut self, id: &uint, node: &N) {
        self.map.insert(id.clone(), node.clone());
    }
}
