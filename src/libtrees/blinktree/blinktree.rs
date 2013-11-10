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
use std::{cast, ptr};


use node::{Node, INode, Leaf};
use statistics::{StatisticsManager, AtomicStatistics};
use storage::{StorageManager, StupidHashmapStorage};
use lock::{LockManager, SimpleLockManager};
use blinktree::node::{BLinkNode, Right, Down, MostLeftDown, MostRightDown};
use blinktree::node::{SimpleBLinkINode, SimpleBLinkLeaf};


// state edge cases

pub static MOST_RIGHT_NODE: u8 = 1 << 0;
pub static MOST_LEFT_NODE: u8  = 1 << 1;
pub static ROOT: u8            = 1 << 2;
pub static ROOT_NEEDS_SPLIT: u8   = 1 << 3;


macro_rules! node_method(
    ($name:ident, $method:ident) => (
        match $name {
            &INode(ref inode) => inode.$method(),
            &Leaf(ref leaf) => leaf.$method()
        }
    );
)
macro_rules! unset(
    ($name:ident, $constant:ident) => (
        $name = $name ^ $constant;
    )
)

macro_rules! update_state(
    ($name:ident, $movement:expr) => (
        match $movement {
            Down => {
                unset!($name, MOST_RIGHT_NODE);
                unset!($name, ROOT);
            }
            MostRightDown => {
                unset!($name, MOST_LEFT_NODE);
                unset!($name, ROOT);
            }
            MostLeftDown => {
                unset!($name, MOST_RIGHT_NODE)
                unset!($name, ROOT);
            }
            Right => {
                unset!($name, MOST_RIGHT_NODE)
                unset!($name, ROOT);
            }
        }
    );
)
pub struct BTree<Ptr, Storage, LockManager, Stats> {
    root: Ptr,
    storage: Storage,
    lock_manager: LockManager,
    statistics: Stats,
    max_size: uint,
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
        self.statistics.elements()
    }
}
impl<K: TotalOrd + Clone + ToStr,
     V: ToStr,
     Ptr: Clone + Eq + ToStr,
     INode:      BLinkNode<K, Ptr, Ptr>,
     Leaf:       BLinkNode<K, V, Ptr>,
     Storage:    StorageManager<Ptr, Node<INode, Leaf>>,
     Locks:      LockManager<Ptr>,
     Stats:      StatisticsManager>
BTree<Ptr, Storage, Locks, Stats> {
    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        let (mut state, mut leaf, _) = self.find_leaf(key);

        while !leaf.can_contain_key(state, key) {
            let current_ptr = match leaf.scannode(state, key) {
                Some((id, _)) => {
                    unset!(state, MOST_LEFT_NODE);
                    id
                }
                None => break
            };
            let current_node = self.storage.read(current_ptr).unwrap();
            leaf = current_node.getLeaf();
        }
        leaf.get(state, key)
    }
}
impl<'self,
    K: Ord + Clone,
    V,
    Ptr: Clone,
    INode:   BLinkNode<K, Ptr, Ptr>,
    Leaf:    BLinkNode<K, V, Ptr>,
    Storage:    StorageManager<Ptr, Node<INode, Leaf>>,
    Locks:      LockManager<Ptr>,
    Stats:      StatisticsManager>
Clone for BTree<Ptr, Storage, Locks, Stats> {
    fn clone(&self) -> BTree<Ptr, Storage, Locks, Stats> {
        unsafe {
            ptr::read_ptr(ptr::to_mut_unsafe_ptr(cast::transmute_mut(self)))
        }
    }
}
impl<'self,
    K: TotalOrd + Clone + ToStr,
    V: ToStr,
    Ptr: Clone + Eq + ToStr,
    INODE:  BLinkNode<K, Ptr, Ptr>,
    LEAF:       BLinkNode<K, V, Ptr>,
    Storage:    StorageManager<Ptr, Node<INODE, LEAF>>,
    Locks:      LockManager<Ptr>,
    Stats:      StatisticsManager>
BTree<Ptr, Storage, Locks, Stats> {
    fn insert(&self, key: K, value: V) {
        let (mut state, leaf, mut visited_nodes) = self.find_leaf(&key);
        let current_ptr = leaf.my_ptr();
        self.lock_manager.lock(leaf.my_ptr().clone());
        let current_node = self.storage.read(current_ptr).unwrap();
        let (mut current_ptr, current_node) = self.move_right(&mut state, current_node, &key);
        unsafe {
            let mut_self = cast::transmute_mut(self);
            let mut insert_res = mut_self.insert_into_leaf(state, current_node, key, value);

            self.statistics.inc_elements();
            while insert_res.is_some() {
                let (key, ptr) = insert_res.unwrap();
                let old_ptr = current_ptr;
                match visited_nodes.pop_opt() {
                    Some(p) => current_ptr = p,
                    None => {
                        if old_ptr == &self.root {   // we need to split the root
                            mut_self.new_root(key, old_ptr, &ptr);
                            return;
                        } else { // root was splitted, need a new visited nodes stack to backtrace
                            let (_,_, visited_stack) = self.find_inode(&key, old_ptr);
                            visited_nodes = visited_stack;
                            visited_nodes.pop();
                            current_ptr = visited_nodes.pop();
                        }
                    }
                }
                if current_ptr == &self.root {
                    state = ROOT;
                }
                self.lock_manager.lock(current_ptr.clone());
                self.lock_manager.unlock(old_ptr);
                let inode = self.storage.read(current_ptr).unwrap();
                insert_res = mut_self.insert_into_inode(state, inode, key, ptr);
            }
        }
        self.lock_manager.unlock(current_ptr);
    }
    fn find_inode<'a>(&'a self, key: &K, inode_ptr: &Ptr) -> (u8, &'a INODE, ~[&'a Ptr]) {
        // bit array to keep track of special events, like if current node is root
        let mut state: u8 = ROOT;
        let mut visited_nodes = ~[];
        let mut current_ptr = &self.root;
        let mut current_node = self.storage.read(current_ptr).unwrap();

        // going the tree down
        while current_ptr != inode_ptr {
            let inode = current_node.getINode();
            match inode.scannode(state, key) {
                // link pointer are not saved for backtracing
                Some((id, Right)) => {
                    update_state!(state, Right);
                    current_ptr = id;
                }
                // was not a link pointer, saving for backtracing
                Some((id, movement)) => {
                    update_state!(state, movement);
                    visited_nodes.push(id);
                    current_ptr = id;
                }
                None => fail!("inconsistent BTree")
            }
            current_node = self.storage.read(current_ptr).unwrap();
        }
        return (state, current_node.getINode(), visited_nodes)
    }


    fn find_leaf<'a>(&'a self, key: &K) -> (u8, &'a LEAF, ~[&'a Ptr]) {
        // bit array to keep track of special events, like if current node is root
        let mut state: u8 = ROOT;
        let mut visited_nodes = ~[&self.root];
        let mut current_ptr = &self.root;
        let mut current_node = self.storage.read(current_ptr).unwrap();

        // going the tree down
        while current_node.isINode() {
            let inode = current_node.getINode();
            match inode.scannode(state, key) {
                // link pointer are not saved for backtracing
                Some((id, Right)) => {
                    update_state!(state, Right);
                    current_ptr = id;
                }
                // was not a link pointer, saving for backtracing
                Some((id, movement)) => {
                    update_state!(state, movement);
                    visited_nodes.push(id);
                    current_ptr = id;
                }
                None => fail!("inconsistent BTree")
            }
            current_node = self.storage.read(current_ptr).unwrap();
        }
        // pops the leaf from the backtrace stack
        visited_nodes.pop_opt();
        return (state, current_node.getLeaf(), visited_nodes)
    }
    // ensures that we are on the node that can contains the key
    fn move_right<'a>(&'a self, state: &mut u8, node: &'a Node<INODE, LEAF>, key: &K)
        -> (&'a Ptr, &'a Node<INODE, LEAF>) {

        let mut current_node = node;
        let mut current_ptr = node_method!(node, my_ptr);
        loop {
            let maybe_right_ptr = match current_node {
                &Leaf(ref leaf) => leaf.move_right(*state, key),
                &INode(ref inode) => inode.move_right(*state, key)
            };
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
    fn insert_into_leaf(&mut self, state: u8, node: &Node<INODE, LEAF>, key: K, value: V) -> Option<(K, Ptr)> {
        let leaf = node.getLeaf();
        let at_least_one_left = self.max_size - 1;
        if !leaf.needs_split(at_least_one_left) {
            unsafe {  // not really, becouse we hold a lock of this node
                cast::transmute_mut(leaf).insert(state, key, value);
                let mut_storage = cast::transmute_mut(&self.storage);
                mut_storage.write(&leaf.my_ptr().clone(), node);
            }
            return None;
        } else {
            let new_child_ptr = self.storage.new_page();
            unsafe {  // not really, becouse we hold a lock of this node
                use std::cast::transmute_mut;

                let new_leaf = transmute_mut(leaf)
                    .split_and_insert(state, new_child_ptr.clone(), key, value);
                let leaf_max_key = leaf.max_key().clone();
                let mut_storage = transmute_mut(&self.storage);
                mut_storage.write(&new_child_ptr, &Leaf(*new_leaf));
                mut_storage.write(&leaf.my_ptr().clone(), node);
                self.statistics.inc_leafs();
                return Some((leaf_max_key, new_child_ptr));
            }
        }
    }
    fn insert_into_inode(&mut self, state: u8, node: &Node<INODE, LEAF>, key: K, ptr: Ptr) -> Option<(K, Ptr)> {
        let inode = node.getINode();
        let at_least_one_left = self.max_size - 1;
        if !inode.needs_split(at_least_one_left) {
            unsafe {  // not really, becouse we hold a lock over this node
                cast::transmute_mut(inode).insert(state, key, ptr);
                cast::transmute_mut(&self.storage).write(&inode.my_ptr().clone(), node);
            }
            return None;
        } else { // split is needed
            let new_page_ptr = self.storage.new_page();

            unsafe {  // not really, becouse we hold a lock of this node
                let new_inode = cast::transmute_mut(inode)
                    .split_and_insert(state, new_page_ptr.clone(), key, ptr);

                let inode_max_key = inode.max_key().clone();
                let mut_storage = cast::transmute_mut(&self.storage);
                mut_storage.write(&new_page_ptr, &INode(*new_inode));
                mut_storage.write(&inode.my_ptr().clone(), node);
                self.statistics.inc_inodes();
                return Some((inode_max_key, new_page_ptr));
            }
        }
    }
    fn new_root(&mut self, key : K, smaller: &Ptr, bigger: &Ptr) {
        let new_root_ptr = self.storage.new_page();
        let root = BLinkNode::new(new_root_ptr.clone(), None, ~[key.clone()], ~[smaller.clone(), bigger.clone()]);
        self.storage.write(&new_root_ptr, &INode(root));

        // we are still holding a lock over the old root, so we can be sure no one else will change
        // the root pointer
        self.root = new_root_ptr;
        self.statistics.inc_inodes();
    }
}
pub fn is_in_state(state: u8, constant: u8) -> bool {
    state & constant == constant
}
#[test]
fn test_is_in_state() {
    assert!(is_in_state(ROOT, ROOT))
    assert!(! is_in_state(!ROOT, ROOT))
    assert!(is_in_state(ROOT | MOST_LEFT_NODE, ROOT))
}
type UintBTree = BTree<uint,
                       StupidHashmapStorage<
                           uint,
                           Node<
                               SimpleBLinkINode<uint,uint, uint>,
                               SimpleBLinkLeaf<uint, uint,uint>
                           >
                       >,
                       SimpleLockManager<uint>,
                       AtomicStatistics>;


impl BTree<uint,
           StupidHashmapStorage<
               uint,
               Node<
                   SimpleBLinkINode<uint,uint, uint>,
                   SimpleBLinkLeaf<uint, uint,uint>
               >
           >,
           SimpleLockManager<uint>,
           AtomicStatistics>
{
        fn new_test() -> UintBTree {
            BTree::new_test_with_size(4)
        }
        fn new_test_with_size(max_size: uint) -> UintBTree {
            let root_ptr = 0;
            let mut storage = StupidHashmapStorage::new();
            let root = BLinkNode::new(root_ptr, None, ~[], ~[]);
            storage.write(&root_ptr, &Leaf(root));
            let btree = BTree {
                root: root_ptr,
                storage: storage,
                lock_manager: SimpleLockManager::new(),
                statistics: AtomicStatistics::new(),
                max_size: max_size
            };
            btree.statistics.inc_leafs();
            btree
        }
    }
#[cfg(test)]
mod test {
    use super::{BTree, UintBTree};
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

    #[test] #[ignore]
    fn test_random_insertion() {
        let btree = BTree::new_test_with_size(4);
        for i in range(1u,1000) {
            let key = random();
            let value = random();
            btree.insert(key, value);
            assert!(btree.len() == i);
            let found = btree.find(&key);
            assert!(found.is_some(), format!("key: {}, value: {}, i: {}", key, value, i));
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
        assert!(root.inode.keys == exp, format!("root.keys {} != {}", root.inode.keys.to_str(), exp.to_str()));
        let leaf = btree.storage.read(&root.inode.values[2]).unwrap().getLeaf();

        assert!(leaf.leaf.keys == ~[5,6,7], format!("leaf.keys {} != [6,7]", leaf.leaf.keys.to_str()));
    }

    #[test]
    fn test_find_after_leaf_split() {
        let btree = BTree::new_test_with_size(4);
        let size_leaf_needs_split = 8;
        insert_range(&btree, 1, size_leaf_needs_split); // insert 1,2,3,4,5,6,7

        assert!(btree.statistics.leafs() == 3);
        assert!(btree.statistics.inodes() == 1);

        let expected = 7;
        assert!(btree.find(&7) == Some(&expected));
    }

    #[bench] #[ignore] // meaningless until we have hard disk storage
    fn bench_range_insert(b: &mut BenchHarness) {
        let btree = BTree::new_test_with_size(1000);
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


