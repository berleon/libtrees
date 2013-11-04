
use std::hash::Hash;
use std::hashmap::HashMap;

pub trait StorageManager<Ptr, N> {
    fn new_page(&self) -> Ptr;
    fn read<'a>(&'a self, id: &Ptr) -> Option<&'a N>;
    fn write(&mut self, id: &Ptr, node: &N);
}

pub struct StupidHashmapStorage<Ptr, N> {
    map: HashMap<Ptr, N>,
}
impl<Ptr: Eq + Hash, N> StupidHashmapStorage<Ptr, N>  {
    pub fn new() -> StupidHashmapStorage<Ptr, N> {
        StupidHashmapStorage {
            map: HashMap::new()
        }
    }
}

impl<Ptr: Clone + Eq + Hash, N: Clone> StorageManager<Ptr, N> for StupidHashmapStorage<Ptr, N> {
    fn new_page(&self) -> Ptr {
        fail!();
    }
    fn read<'a>(&'a self, id: &Ptr) -> Option<&'a N> {
        self.map.find(id)
    }
    fn write(&mut self, id: &Ptr, node: &N) {
        self.map.insert(id.clone(), node.clone());
    }
}