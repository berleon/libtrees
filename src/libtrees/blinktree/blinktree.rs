use std::container::{Container, Map};
use std::cast;

use node::{Node, INode, Leaf};
use statistics::{StatisticsManager, AtomicStatistics};
use storage::{StorageManager, StupidHashmapStorage};
use lock::{LockManager, SimpleLockManager};
use blinktree::node::{BLinkNode, Right, Down};

struct BTree<Ptr, Storage, LockManager, Stats> {
    root: Ptr,
    storage: Storage,
    lock_manager: LockManager,
    statistics: Stats
}

impl<'self,
    K,
    V,
    Ptr: Clone,
    INode: BLinkNode<K,Ptr,Ptr>,
    Leaf: BLinkNode<K,V,Ptr>,
    Storage: StorageManager<Ptr, Node<INode, Leaf>>,
    Locks: LockManager<Ptr>,
    Stats: StatisticsManager> Container for BTree<Ptr, Storage, Locks, Stats> {
    fn len(&self) -> uint {
        self.statistics.size()
    }
}
impl<'self,
    K: Ord,
    V,
    Ptr: Clone,
    InnerNode:  BLinkNode<K, Ptr, Ptr>,
    Leaf:       BLinkNode<K, V, Ptr>,
    Storage:    StorageManager<Ptr, Node<InnerNode, Leaf>>,
    Locks:      LockManager<Ptr>,
    Stats:      StatisticsManager>
Map<K, V> for BTree<Ptr, Storage, Locks, Stats> {
    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        let mut current_ptr = &self.root;

        let mut current_node = self.storage.read(current_ptr).unwrap();

        while current_node.isINode() {
            let inode = current_node.getINode();
            match inode.scannode(key) {
                Some((id, _)) => current_ptr = id,
                None => return None
            }
            current_node = self.storage.read(current_ptr).unwrap();
        }
        assert!(current_node.isLeaf());

        let mut leaf = current_node.getLeaf();
        while !leaf.can_contain(key) {
            match leaf.scannode(key) {
                Some((id, _)) => current_ptr = id,
                None => break
            };

            current_node = self.storage.read(current_ptr).unwrap();
            leaf = current_node.getLeaf();
        }
        leaf.get(key)
    }
}
impl<'self,
    K: Ord + Clone,
    V,
    Ptr: Clone,
    InnerNode:  BLinkNode<K, Ptr, Ptr>,
    Leaf:       BLinkNode<K, V, Ptr>,
    Storage:    StorageManager<Ptr, Node<InnerNode, Leaf>>,
    Locks:      LockManager<Ptr>,
    Stats:      StatisticsManager>
BTree<Ptr, Storage, Locks, Stats> {
    fn insert(&self, key: K, value: V) {
        let mut stack = ~[];
        let mut current_ptr = &self.root;
        let mut current_node = self.storage.read(current_ptr).unwrap();

        // going the tree down
        while current_node.isINode() {
            let inode = current_node.getINode();
            let tmp = current_ptr;
            match inode.scannode(&key) {
                // link pointer are not saved for backtracing
                Some((id, Down)) =>
                    current_ptr = id,
                // was not a link pointer, saving for backtracing
                Some((id, Right)) => {
                    stack.push(id);
                    current_ptr = id;
                }
                _ => return
            }
            current_node = self.storage.read(current_ptr).unwrap();
        }

        // current_ptr is candidate leaf
        self.lock_manager.lock(current_ptr.clone());
        current_node = self.storage.read(current_ptr).unwrap();

        current_node = self.move_right(current_node, &key);
        let leaf = current_node.getLeaf();

        if !leaf.needs_split() {
            unsafe {  // not really, becouse we hold a lock of this node
                cast::transmute_mut(leaf).insert(key, value);
                let mut_storage = cast::transmute_mut(&self.storage);
                mut_storage.write(leaf.my_ptr(), current_node);

            }
            self.lock_manager.unlock(current_ptr);
            return;
        } else {
            let mut new_child_ptr = self.storage.new_page();
            let mut max_key = leaf.max_key();

            unsafe {  // not really, becouse we hold a lock of this node
                let mut_leaf = cast::transmute_mut(leaf);
                let new_leaf = mut_leaf.split_and_insert(new_child_ptr.clone(), key, value);
                let mut_storage = cast::transmute_mut(&self.storage);
                mut_storage.write(&new_child_ptr, &Leaf(*new_leaf));
                mut_storage.write(current_ptr, current_node);
            }
            let old_ptr = current_ptr;
            current_ptr = stack.pop();
            self.lock_manager.lock(current_ptr.clone());
            self.lock_manager.unlock(old_ptr);

            let mut max_key_child = leaf.max_key();
            loop {
                current_node = self.storage.read(current_ptr).unwrap();
                let inode = current_node.getINode();
                if !inode.needs_split() {
                    unsafe {  // not really, becouse we hold a lock over this node
                        cast::transmute_mut(inode).insert(max_key.clone(), new_child_ptr);
                        let mut_storage = cast::transmute_mut(&self.storage);
                        mut_storage.write(inode.my_ptr(), &Leaf(cast::transmute(inode)));
                    }
                    self.lock_manager.unlock(current_ptr);
                    return;
                } else { // split is needed
                    let new_page_ptr = self.storage.new_page();

                    unsafe {  // not really, becouse we hold a lock of this node
                        let mut_inode = cast::transmute_mut(inode);
                        let new_inode = mut_inode.split_and_insert(new_page_ptr.clone(),
                                                                 max_key_child.clone(),
                                                                 new_child_ptr.clone());
                        max_key = inode.max_key();
                        let mut_storage = cast::transmute_mut(&self.storage);
                        mut_storage.write(&new_page_ptr, &INode(*new_inode));
                        mut_storage.write(current_ptr, current_node);
                    }
                    let old_ptr = current_ptr;
                    current_ptr = stack.pop();

                    self.lock_manager.lock(current_ptr.clone());
                    self.lock_manager.unlock(old_ptr);
                }
            }
        }
    }
    fn move_right<'a>(&'a self, node: &'a Node<InnerNode, Leaf>, key: &K)
        -> &'a Node<InnerNode, Leaf>{

        let mut current_node = node;
        loop {
            let (current_ptr, right_ptr) =
                if node.isLeaf() {
                    let leaf = node.getLeaf();
                    (leaf.my_ptr(), leaf.move_right(key))
                } else {
                    let inode = node.getINode();
                    (inode.my_ptr(), inode.move_right(key))
                };
            match right_ptr {
                Some(ptr) => {
                    self.lock_manager.lock(ptr.clone());
                    self.lock_manager.unlock(current_ptr);
                    let current_ptr = ptr;
                    current_node = self.storage.read(current_ptr).unwrap();
                }
                _ => break
            }
        }
        current_node
    }
}
impl<'self, K,V,
    K: Ord,
    V,
    Ptr: Eq + Hash + Clone,
    InnerNode:  BLinkNode<K, Ptr, Ptr>,
    Leaf:       BLinkNode<K, V, Ptr>>
BTree<Ptr,
      StupidHashmapStorage<Ptr, Node<InnerNode,Leaf>>,
      SimpleLockManager<Ptr>,
      AtomicStatistics> {
    fn new_test(root_ptr: Ptr) -> BTree<Ptr,
                           StupidHashmapStorage<Ptr, Node<InnerNode,Leaf>>,
                           SimpleLockManager<Ptr>,
                           AtomicStatistics>
    {
        BTree {
            root: root_ptr,
            storage: StupidHashmapStorage::new(),
            lock_manager: SimpleLockManager::new(),
            statistics: AtomicStatistics::new()
        }
    }
}

#[cfg(test)]
mod test {

}


