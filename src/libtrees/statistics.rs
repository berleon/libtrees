
use std::unstable::atomics::{AtomicUint, Ordering, Relaxed};
use std::cast;

pub trait StatisticsManager {
    fn size(&self) -> uint;
    fn incsize(&self);
    fn decsize(&self);
}
pub struct AtomicStatistics {
    size: AtomicUint
}

impl StatisticsManager for AtomicStatistics {
    fn size(&self) -> uint {
        self.size.load(Relaxed)
    }
    fn incsize(&self){
        unsafe {
            cast::transmute_mut(&self.size).fetch_add(1, Relaxed);
        }
    }
    fn decsize(&self){
        unsafe {
            cast::transmute_mut(&self.size).fetch_sub(1, Relaxed);
        }
    }
}
impl AtomicStatistics {
    pub fn new() -> AtomicStatistics {
        AtomicStatistics {
            size: AtomicUint::new(0)
        }

    }
}
