use std::container::{Container, Map};

use node::Node;
use statistics::{StatisticsManager, NotSyncedStatistics};
use storage::{StorageManager, StupidHashmapStorage};

struct BTree<NodeStorage, ValueStorage, Stats> {
    root: uint,
    node_storage: NodeStorage,
    data_storage: ValueStorage,
    statistics: Stats
}

impl<'self,
    K,
    V,
    N: Node<K,V>,
    Nodestorage: StorageManager<N>,
    Datastorage: StorageManager<V>,
    Stats: StatisticsManager> Container for BTree<Nodestorage, Datastorage, Stats> {
    fn len(&self) -> uint {
        self.statistics.size()
    }
}
impl<'self,
    K: Ord,
    V,
    N: Node<K,V>,
    NodeStorage: StorageManager<N>,
    DataStorage: StorageManager<V>,
    Stats: StatisticsManager> Map<K, V> for BTree<NodeStorage, DataStorage, Stats> {
    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        let mut current = self.root;
        let mut current_node = self.node_storage.read(current).unwrap();
        while !current_node.isLeaf() {
            match current_node.scannode(key) {
                Some((id, _)) => current = id,
                None => return None
            }
            current_node = self.node_storage.read(current).unwrap();
        }
        assert!(current_node.isLeaf());
        while !current_node.can_contains(key) {
            match current_node.scannode(key) {
                Some((id, _)) => current = id,
                None => return None
            }

            current_node = self.node_storage.read(current).unwrap()
        }
        current_node.get(key)
    }
}
impl<'self,
    K: Ord,
    V,
    N: Node<K,V>,
    NodeStorage: StorageManager<N>,
    DataStorage: StorageManager<V>,
    Stats: StatisticsManager> BTree<NodeStorage, DataStorage, Stats> {
    fn insert(&self, key: K, value: V) {
        let mut stack = ~[];
        let mut current = self.root;
        let mut current_node = self.node_storage.read(current).unwrap();
        while !current_node.isLeaf() {
            let tmp = current;
            match current_node.scannode(&key) {
                // link pointer are not saved for backtracing
                Some((id, true)) =>
                    current = id,
                // was not a link pointer, saving for backtracing
                Some((id, false)) => {
                    stack.push(id);
                    current = id;
                }
                _ => return
            }
            current_node = self.node_storage.read(current).unwrap();
        }
        
    }
}
impl<'self, K,V,
    N: Node<K,V>> BTree<StupidHashmapStorage<N>,
           StupidHashmapStorage<V>,
           NotSyncedStatistics> {
    fn new_test() -> BTree<StupidHashmapStorage<N>,
                                StupidHashmapStorage<V>,
                                NotSyncedStatistics>{
        BTree {
            root: 0,
            data_storage: StupidHashmapStorage::new(),
            node_storage: StupidHashmapStorage::new(),
            statistics: NotSyncedStatistics::new()
        }
    }
}
