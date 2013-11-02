
use std::hashmap::HashMap;

pub trait StorageManager<N> {
    fn read<'a>(&'a self, id: uint) -> Option<&'a N>;
    fn write(&mut self, id: uint, node: N);
}

pub struct StupidHashmapStorage<N> {
    map: HashMap<uint, N>,
}
impl<N> StupidHashmapStorage<N>  {
    pub fn new() -> StupidHashmapStorage<N> {
        StupidHashmapStorage {
            map: HashMap::new()
        }
    }
}

impl<N> StorageManager<N> for StupidHashmapStorage<N> {
    fn read<'a>(&'a self, id: uint) -> Option<&'a N> {
        self.map.find(&id)
    }
    fn write(&mut self, id: uint, node: N) {
        self.map.insert(id, node);
    }
}