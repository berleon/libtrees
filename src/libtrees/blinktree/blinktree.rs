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


use std::container::{Container};
use std::cast;

use lock::{LockManager, SimpleLockManager};
use node::{Node, INode, Leaf};
use persistent;
use statistics::{StatisticsManager, AtomicStatistics};
use storage::{StorageManager, StupidHashmapStorage};
use blinktree::blink_ops::{BLinkOps, DefaultBLinkOps, Right, Down};
use blinktree::physical_node::{PhysicalNode, DefaultBLinkNode, T_INODE, T_LEAF};

macro_rules! node_method(
    ($name:ident, $method:ident) => (
        match $name {
            &INode(ref inode) => inode.$method(),
            &Leaf(ref leaf) => leaf.$method()
        }
    );
)

pub struct BTree<Ptr, Storage, LockManager, Stats, BLinkOps> {
    root: Ptr,
    storage: Storage,
    lock_manager: LockManager,
    statistics: Stats,
    max_size: uint,
    ops: BLinkOps
}

impl<'self,
    K,
    V,
    Ptr: Clone,
    INode: PhysicalNode<K,Ptr,Ptr>,
    Leaf: PhysicalNode<K,V,Ptr>,
    OPS : BLinkOps<K,V,Ptr, INode, Leaf>,
    Storage: StorageManager<Ptr, Node<INode, Leaf>>,
    Locks: LockManager<Ptr>,
    Stats: StatisticsManager> Container for BTree<Ptr, Storage, Locks, Stats, OPS> {
    fn len(&self) -> uint {
        self.statistics.elements()
    }
}
impl<K: TotalOrd + Clone + ToStr,
     V: ToStr,
     Ptr: Clone + Eq + ToStr,
     INODE:      PhysicalNode<K, Ptr, Ptr>,
     LEAF:       PhysicalNode<K, V, Ptr>,
     OPS : BLinkOps<K,V,Ptr, INODE, LEAF>,
     Storage:    StorageManager<Ptr, Node<INODE, LEAF>>,
     Locks:      LockManager<Ptr>,
     Stats:      StatisticsManager>
persistent::Map<K,V>
for BTree<Ptr, Storage, Locks, Stats, OPS> {
    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        let (mut current_node, _) = self.find_leaf(key);
        while !self.ops.can_contain_key(current_node.getLeaf(), key) {
            let (current_ptr,_) = self.ops.scannode(current_node, key).unwrap();
            current_node = self.storage.read(current_ptr).unwrap();
        }
        self.ops.get_value(current_node.getLeaf(), key)
    }

    fn insert(&self, key: K, value: V) {
        let (leaf, mut visited_nodes) = self.find_leaf(&key);
        let current_ptr = leaf.my_ptr();
        self.lock_manager.lock(leaf.my_ptr().clone());
        let current_node = self.read(current_ptr);
        let (mut current_ptr, mut current_node) = self.move_right(current_node, &key);
        unsafe {
            let mut_self = cast::transmute_mut(self);
            let mut insert_res = mut_self.insert_into_leaf(current_node, key, value);

            self.statistics.inc_elements();
            while insert_res.is_some() {
                let (key, ptr) = insert_res.unwrap();
                let old_ptr = current_ptr;
                match visited_nodes.pop_opt() {
                    Some(p) => current_ptr = p,
                    None => {
                        if old_ptr == &self.root {   // we need to split the root
                            mut_self.new_root(cast::transmute_mut(current_node), key, old_ptr, &ptr);
                            break;
                        } else { // root was splitted, need a new visited nodes stack to backtrace
                            let (_, visited_stack) = self.find_node(&key, |n| {n.my_ptr() == old_ptr});
                            visited_nodes = visited_stack;
                            visited_nodes.pop();
                            current_ptr = visited_nodes.pop();
                        }
                    }
                }
                self.lock_manager.lock(current_ptr.clone());
                self.lock_manager.unlock(old_ptr);
                current_node = self.read(current_ptr);
                insert_res = mut_self.insert_into_inode(current_node, key, ptr);
            }
        }
        self.statistics.inc_insertions();
        self.lock_manager.unlock(current_ptr);
    }
    #[allow(unused_variable)]
    fn remove(&self, key: &K) {
        fail!();
    }
}

impl<K: TotalOrd + Clone + ToStr,
     V: ToStr,
     Ptr: Clone + Eq + ToStr,
     INODE:      PhysicalNode<K, Ptr, Ptr>,
     LEAF:       PhysicalNode<K, V, Ptr>,
     OPS : BLinkOps<K,V,Ptr, INODE, LEAF>,
     Storage:    StorageManager<Ptr, Node<INODE, LEAF>>,
     Locks:      LockManager<Ptr>,
     Stats:      StatisticsManager>
BTree<Ptr, Storage, Locks, Stats, OPS> {
    fn find_node<'a>(&'a self, key: &K, predicate: &fn(n : &Node<INODE, LEAF>) -> bool)
        -> (&'a Node<INODE,LEAF>, ~[&'a Ptr]) {
        let mut visited_nodes = ~[&self.root];
        let mut current_ptr = &self.root;
        let mut current_node = self.read(current_ptr);
        // going the tree down
        while current_node.isINode() && predicate(current_node) {
            match self.ops.scannode(current_node, key) {
                // link pointer are not saved for backtracing
                Some((id, Right)) => {
                    current_ptr = id;
                }
                // was not a link pointer, saving for backtracing
                Some((id, Down)) => {
                    visited_nodes.push(id);
                    current_ptr = id;
                }
                None => fail!("inconsistent BTree")
            }
            current_node = self.storage.read(current_ptr).unwrap();
        }
        // pops the leaf from the backtrace stack
        visited_nodes.pop_opt();
        return (current_node, visited_nodes)
    }
    fn find_leaf<'a>(&'a self, key: &K) -> (&'a Node<INODE,LEAF>, ~[&'a Ptr]) {
        self.find_node(key, |_| {true})
    }
    // ensures that we are on the node that can contains the key
    fn move_right<'a>(&'a self, node: &'a Node<INODE, LEAF>, key: &K)
        -> (&'a Ptr, &'a Node<INODE, LEAF>) {

        let mut current_node = node;
        let mut current_ptr = node.my_ptr();
        loop {
            let maybe_right_ptr = self.ops.move_right(current_node, key);
            match maybe_right_ptr {
                Some(ptr) => {
                    self.lock_manager.lock(ptr.clone());
                    self.lock_manager.unlock(current_ptr);
                    current_ptr = ptr;
                    current_node = self.storage.read(current_ptr).unwrap();
                }
                _ => break
            }
        }
        (current_ptr, current_node)
    }

    // if a split was necessary, it returns the pointer and the minimum key of the new leaf.
    // this method mutates the tree. call it only, if you hold a lock of the `node`.
    fn insert_into_leaf(&mut self, node: &Node<INODE, LEAF>, key: K, value: V) -> Option<(K, Ptr)> {
        let leaf = node.getLeaf();
        let at_least_one_left = self.max_size - 1;
        if !leaf.needs_split(at_least_one_left) {
            unsafe {  // not really, becouse we hold a lock of this node
                self.ops.insert_leaf(cast::transmute_mut(leaf), key, value);
                let mut_storage = cast::transmute_mut(&self.storage);
                mut_storage.write(&leaf.my_ptr().clone(), node);
            }
            return None;
        } else {
            debug!("[insert_into_leaf] spliting: {}", leaf.keys().to_str());
            let new_child_ptr = self.storage.new_page();
            unsafe {  // not really, becouse we hold a lock of this node
                use std::cast::transmute_mut;

                let new_leaf = self.ops.split_and_insert_leaf(
                    transmute_mut(leaf), new_child_ptr.clone(), key, value);
                let leaf_max_key = leaf.max_key().clone();
                let mut_storage = transmute_mut(&self.storage);
                mut_storage.write(&new_child_ptr, &Leaf(new_leaf));
                mut_storage.write(&leaf.my_ptr().clone(), node);
                self.statistics.inc_leafs();
                return Some((leaf_max_key, new_child_ptr));
            }
        }
    }
    fn insert_into_inode(&mut self, node: &Node<INODE, LEAF>, key: K, ptr: Ptr) -> Option<(K, Ptr)> {
        let inode = node.getINode();
        let at_least_one_left = self.max_size - 1;
        if !inode.needs_split(at_least_one_left) {
            unsafe {  // not really, becouse we hold a lock over this node
                self.ops.insert_inode(cast::transmute_mut(inode), key, ptr);
                cast::transmute_mut(&self.storage).write(&inode.my_ptr().clone(), node);
            }
            return None;
        } else { // split is needed
            let new_page_ptr = self.storage.new_page();

            unsafe {  // not really, becouse we hold a lock of this node
                let new_inode = self.ops.split_and_insert_inode(
                    cast::transmute_mut(inode), new_page_ptr.clone(), key, ptr);

                let inode_max_key = inode.max_key().clone();
                let mut_storage = cast::transmute_mut(&self.storage);
                mut_storage.write(&new_page_ptr, &INode(new_inode));
                mut_storage.write(&inode.my_ptr().clone(), node);
                self.statistics.inc_inodes();
                return Some((inode_max_key, new_page_ptr));
            }
        }
    }
    fn new_root(&mut self, current_node: &mut Node<INODE,LEAF>, key : K, smaller: &Ptr, bigger: &Ptr) {
        debug!("new root key: {}", key.to_str());
        let new_root_ptr = self.storage.new_page();
        let root = PhysicalNode::new(T_INODE, new_root_ptr.clone(), None,
                                     ~[key.clone()], ~[smaller.clone(), bigger.clone()]);
        self.storage.write(&new_root_ptr, &INode(root));

        // we are still holding a lock over the old root, so we can be sure no one else will change
        // the root pointer
        self.root = new_root_ptr;
        current_node.unset_root();
        self.statistics.inc_inodes();
    }
    fn read<'a>(&'a self, ptr: &Ptr) -> &'a Node<INODE, LEAF> {
        use std::cast;
        let node = self.storage.read(ptr).unwrap();
        if ptr == &self.root {
            unsafe {
                cast::transmute_mut(node).set_root();
            }
        }
        return node;
    }
}

type UintBTree = BTree<uint,
                       StupidHashmapStorage<
                           uint,
                           Node<
                               DefaultBLinkNode<uint,uint, uint>,
                               DefaultBLinkNode<uint, uint,uint>
                           >
                       >,
                       SimpleLockManager<uint>,
                       AtomicStatistics,
                       DefaultBLinkOps<uint,uint,uint,
                           DefaultBLinkNode<uint,uint,uint>,
                           DefaultBLinkNode<uint, uint, uint>>
>;


impl BTree<uint,
           StupidHashmapStorage<
               uint,
               Node<
                   DefaultBLinkNode<uint,uint, uint>,
                   DefaultBLinkNode<uint, uint,uint>
               >
           >,
           SimpleLockManager<uint>,
           AtomicStatistics,
           DefaultBLinkOps<uint,uint,uint,
                           DefaultBLinkNode<uint,uint,uint>,
                           DefaultBLinkNode<uint, uint, uint>>
>
{
    fn new_test() -> UintBTree {
        BTree::new_test_with_size(4)
    }
    fn new_test_with_size(max_size: uint) -> UintBTree {
        let root_ptr = 0;
        let mut storage = StupidHashmapStorage::new();
        let root = PhysicalNode::new(T_LEAF, root_ptr, None, ~[], ~[]);
        storage.write(&root_ptr, &Leaf(root));
        let btree = BTree {
            root: root_ptr,
            storage: storage,
            lock_manager: SimpleLockManager::new(),
            statistics: AtomicStatistics::new(),
            max_size: max_size,
            ops: DefaultBLinkOps
        };
        btree.statistics.inc_leafs();
        btree
    }
}
#[cfg(test)]
mod test {
    use super::{BTree, UintBTree};
    use persistent::Map;
    use std::rand::random;
    use extra::test::BenchHarness;

    fn insert_range(btree: &UintBTree, from: uint, to: uint) {
        for i in range(from,to) {
            let key = i;
            let value = i;
            btree.insert(key, value);
        }
    }

    #[test]
    fn test_insert() {
        let btree = BTree::new_test();

        btree.insert(3, 3);
        let res = btree.find(&3);
        assert!(res.is_some());

        assert!(btree.find(&2).is_none());

        btree.insert(2, 5);
        let expected = &5;
        assert!(btree.find(&2) == Some(expected));


        btree.insert(4, 5);
        assert!(btree.find(&4).is_some());
        let expected = &5;
        assert!(btree.find(&4) == Some(expected));


        btree.insert(1204260299403256469, 17554158702358192490);
        assert!(btree.find(&1204260299403256469).is_some());
        assert!(btree.statistics.elements() == 4);
    }
    #[test]
    fn test_new_root() {
        let max_size = 4;
        let btree = BTree::new_test_with_size(max_size);
        let old_root = btree.root;
        let overflow_root = max_size + 2;
        insert_range(&btree, 1, overflow_root); // insert 1,2,3,4,5
        assert!(btree.root != old_root);

        let overflow_first_level = 14;
        let old_root = btree.root;
        insert_range(&btree, overflow_root, overflow_first_level); // insert 1,2,3,4,5
        assert!(btree.root != old_root);
    }
    #[test]
    fn test_range_insertion() {
        let btree = BTree::new_test_with_size(4);
        for i in range(1u,1000) {
            let key = i;
            let value = i;
            btree.insert(key, value);
            assert!(btree.len() == i);
            let found = btree.find(&key);
            assert!(found.is_some(), format!("key: {}, value: {}, i: {}", key, value, i));
            assert!(found == Some(&value));
        }
    }

    #[test]
    fn test_long_insertion() {
        let btree = BTree::new_test_with_size(4);
        let keys_values: ~[(uint, uint)] = ~[ (123u,344u), (431,78), (134,789),(2,30),(103,104),(853,10), (343,0), (0,103), (13,54), (309,844), (567,999),(898,78),(211,234)];
        for &(key,value) in keys_values.iter() {
            btree.insert(key, value);
            let found = btree.find(&key);
            assert!(found.is_some(), format!("key: {}, value: {}", key, value));
            assert!(found == Some(&value));
        }
    }
    #[test]
    fn test_random_insertion() {
        let btree = BTree::new_test_with_size(4);
        for _ in range(0,10000) {
            let r1: uint= random();
            let r2: uint= random();
            let key = r1 % 100000;
            let value = r2 % 100000;
            btree.insert(key, value);
            let found = btree.find(&key);
            assert!(found.is_some(), format!("key: {}, value: {}", key, value));
            assert!(found == Some(&value));
        }
    }

    #[test]
    fn test_leaf_split() {
        let btree = BTree::new_test_with_size(4);
        let size_leaf_needs_split = 7;
        insert_range(&btree, 1, size_leaf_needs_split); // insert 1,2,3,4,5,6
        btree.insert(7,7);
        let root = btree.storage.read(&btree.root).unwrap().getINode();
        assert!(btree.statistics.leafs() == 3);
        assert!(btree.statistics.inodes() == 1);

        let exp = ~[2,4];
        assert!(root.keys == exp, format!("root.keys {} != {}", root.keys.to_str(), exp.to_str()));
        let leaf = btree.storage.read(&root.values[2]).unwrap().getLeaf();

        assert!(leaf.keys == ~[5,6,7], format!("leaf.keys {} != [6,7]", leaf.keys.to_str()));
    }
    #[test]
    fn test_find() {
        use std::cast;
        let btree = BTree::new_test_with_size(4);
        unsafe {
            let root = cast::transmute_mut(btree.storage.read(&btree.root).unwrap().getLeaf());
            btree.ops.insert_leaf(root, 1u,2u);
            btree.ops.insert_leaf(root, 3u,5u);
            btree.ops.insert_leaf(root, 4u,9u);
            let expected = &2;
            assert!(btree.find(&1) == Some(expected));
            let expected = &5;
            assert!(btree.find(&3) == Some(expected));
            let expected = &9;
            assert!(btree.find(&4) == Some(expected));
        }
    }
    #[test]
    fn test_find_after_leaf_split() {
        let btree = BTree::new_test_with_size(4);
        let size_leaf_needs_split = 5;
        insert_range(&btree, 1, size_leaf_needs_split+1); // insert 1,2,3,4,5

        assert!(btree.statistics.leafs() == 2, format!("{} != 2", btree.statistics.leafs()));
        assert!(btree.statistics.inodes() == 1);

        let expected = 5;
        assert!(btree.find(&5) == Some(&expected));
    }

    #[bench]  #[ignore] // meaningless until we have hard disk storage
    fn bench_range_insert(b: &mut BenchHarness) {
        let btree = BTree::new_test_with_size(4);
        let mut i = 0;
        do b.iter {
            btree.insert(i, i);
            i += 1;
        }
    }
    /*
    #[test] #[ignore]
    fn test_concurrent() {
        let btree = ~BTree::new_test();
        let arc_btree = Arc::new(btree);
        for i in range(0,10) {
            let local_btree = arc_btree.clone();
            let mut task = task::task();
            task.watched();
            do task.spawn {
                for i in range(0,1000) {
                    local_btree.get().insert(random(), random());
                }
            }
        }
    }
    */
}


