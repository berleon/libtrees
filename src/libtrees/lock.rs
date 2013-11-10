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


use std::hashmap::HashSet;
use std::cast;
use extra::sync::Mutex;

pub trait LockManager<T> {
    fn lock(&self, id: T);
    fn unlock(&self, id: &T);
}

struct SimpleLockManager<T> {
    set: HashSet<T>,
    mutex: Mutex
}

impl<T: Hash + Eq + Freeze> SimpleLockManager<T> {
    pub fn new() -> SimpleLockManager<T> {
        SimpleLockManager {
            set: HashSet::new(),
            mutex: Mutex::new()
        }
    }
}
impl<T: Hash + Eq + Clone + Freeze> LockManager<T> for SimpleLockManager<T> {
    fn lock(&self, id: T) {
        do self.mutex.lock_cond |cond| {
            while self.set.contains(&id) {
                cond.wait();
            }
            unsafe {
                let mut_set = cast::transmute_mut(&self.set);
                mut_set.insert(id.clone());
            }
        }
    }
    fn unlock(&self, id: &T) {
        do self.mutex.lock_cond |cond| {
            unsafe {
                let mut_set = cast::transmute_mut(&self.set);
                mut_set.remove(id);
                cond.signal();
            }
        }
    }
}
