
use extra::sync::Mutex;
trait LockManager {
    fn lock(&mut self, id: uint);
    fn unlock(&mut self, id: uint);
}

struct SimpleLockManager<M> {
    map: M,
    mutex: Mutex
}
impl <M: Set<uint> + MutableSet<uint>> LockManager
for SimpleLockManager<M> {
    fn lock(&mut self, id: uint) {
        do self.mutex.lock_cond |cond| {
            while self.map.contains(&id) {
                cond.wait();
            }
            self.map.insert(id);
        }
    }
    fn unlock(&mut self, id: uint) {
        do self.mutex.lock || {
            self.map.remove(&id);
        }
    }
}
