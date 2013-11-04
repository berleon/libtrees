use std::hashmap::HashSet;
use std::ptr;
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

impl<T: Hash + Eq> SimpleLockManager<T> {
    pub fn new() -> SimpleLockManager<T> {
        SimpleLockManager {
            set: HashSet::new(),
            mutex: Mutex::new()
        }
    }
}
impl<T: Hash + Eq + Clone> LockManager<T> for SimpleLockManager<T> {
    fn lock(&self, id: T) {
        do self.mutex.lock_cond |cond| {
            while self.set.contains(&id) {
                cond.wait();
            }
            unsafe {
                let mut mut_set = cast::transmute_mut(&self.set);
                mut_set.insert(id.clone());
            }
        }
    }
    fn unlock(&self, id: &T) {
        do self.mutex.lock_cond |cond| {
            unsafe {
                let mut mut_set = cast::transmute_mut(&self.set);
                mut_set.remove(id);
                cond.signal();
            }
        }
    }
}
