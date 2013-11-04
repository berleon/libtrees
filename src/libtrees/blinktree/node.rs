use std;
use std::to_bytes::IterBytes;

use algorithm;
use node::{Node, INode, Leaf};
use utils;
pub enum Movement {
    Right,
    Down
}
pub trait BLinkNode<K, V, Ptr> {
    fn is_leaf(&self) -> bool;
    fn is_inode(&self) -> bool;

    // use this ptr to point to this node
    fn my_ptr<'a>(&'a self) -> &'a Ptr;
    /// returns the my_ptr of the next node to scan and true if we went of a link ptr
    fn scannode<'a>(&'a self, key: &K) -> Option<(&'a Ptr, Movement)>;
    fn link_ptr<'a>(&'a self) -> Option<&'a Ptr>;
    fn move_right<'a>(&'a self, key: &K) -> Option<&'a Ptr>;

    fn max_key<'a>(&'a self) -> &'a K;
    fn min_key<'a>(&'a self) -> &'a K;

    /// true if key is within the minimum and maximum of this page
    fn can_contain(&self, key: &K) -> bool;
    fn get<'a>(&'a self, key: &K) -> Option<&'a V>;

    fn needs_split(&self) -> bool;
    fn split_and_insert(&mut self, new_page: Ptr, key: K, value: V) -> ~Self;
    fn insert(&mut self, key: K, value: V);
}



struct SimpleBlinkLeaf<K, V, Ptr> {
    my_ptr: Ptr,
    keys: ~[K],
    values: ~[V],
    right: Option<Ptr>
}

struct SimpleBlinkINode<K, Ptr> {
    my_ptr: Ptr,
    keys: ~[K],
    values: ~[Ptr],
    right: Option<Ptr>
}


impl<K: TotalOrd, Ptr>
BLinkNode<K, Ptr, Ptr> for SimpleBlinkINode<K, Ptr> {
    fn my_ptr<'a>(&'a self) -> &'a Ptr {
        return &self.my_ptr;
    }
    fn is_inode(&self) -> bool { true }
    fn is_leaf(&self)  -> bool { false }

    fn scannode<'a>(&'a self, key: &K) -> Option<(&'a Ptr, Movement)> {
        if(! self.can_contain(key)) {
            return self.right.as_ref().map(|r| (r, Right));
        }
        // FIXME
        None
        //self.get(key).map(|r| (r, Down))
    }
    fn link_ptr<'a>(&'a self) -> Option<&'a Ptr> {
        match self.right {
            Some(ref r) => Some(r),
            None => None
        }
    }

    fn move_right<'a>(&'a self, key: &K) -> Option<&'a Ptr> {
        if(! self.can_contain(key)) {
            self.right.as_ref()
        } else {
            None
        }
    }

    fn get<'a>(&'a self, key: &K) -> Option<&'a Ptr> {
        if !self.can_contain(key) {
            return None;
        }
        let my_ptrx = algorithm::bsearch_idx(self.keys, key);
        Some(&self.values[my_ptrx])
    }
    fn can_contain(&self, key: &K) -> bool {
        ( self.keys[0].cmp(key) == Greater ||
          self.keys[0].cmp(key) == Equal )&&
        ( key.cmp(&self.keys[self.keys.len()-1]) == Less||
          key.cmp(&self.keys[self.keys.len()-1]) == Equal )
    }
    fn needs_split(&self) -> bool {
        false
    }
    fn split_and_insert(&mut self, new_page: Ptr, key: K, value: Ptr)
        -> ~SimpleBlinkINode<K, Ptr> {
        fail!()
    }

    fn insert(&mut self, key: K, value: Ptr) { fail!() }

    fn max_key<'a>(&'a self) -> &'a K { fail!() }
    fn min_key<'a>(&'a self) -> &'a K { fail!() }


}

impl<'self,
    K: TotalOrd,
    V: Clone,
    Ptr: Clone>
BLinkNode<K,V, Ptr> for SimpleBlinkLeaf<K, V, Ptr> {
    fn my_ptr<'a>(&'a self) -> &'a Ptr {
        return &self.my_ptr;
    }

    fn is_inode(&self) -> bool { true }
    fn is_leaf(&self)  -> bool { false }


    fn scannode<'a>(&'a self, key: &K) -> Option<(&'a Ptr, Movement)> {
        if(! self.can_contain(key)) {
            self.right.as_ref().map(|r| (r, Right))
        } else {
            None
        }
    }
    fn link_ptr<'a>(&'a self) -> Option<&'a Ptr> {
        match self.right {
            Some(ref r) => Some(r),
            None => None
        }
    }

    fn move_right<'a>(&'a self, key: &K) -> Option<&'a Ptr> {
        if(! self.can_contain(key)) {
            self.right.as_ref()
        } else {
            None
        }
    }

    fn get<'a>(&'a self, key: &K) -> Option<&'a V> {
        if !self.can_contain(key) {
            return None;
        }
        let my_ptrx = algorithm::bsearch_idx(self.keys, key);
        Some(&self.values[my_ptrx])
    }
    fn can_contain(&self, key: &K) -> bool {
        ( self.keys[0].cmp(key) == Greater ||
          self.keys[0].cmp(key) == Equal )&&
        ( key.cmp(&self.keys[self.keys.len()-1]) == Less||
          key.cmp(&self.keys[self.keys.len()-1]) == Equal )
    }
    fn needs_split(&self) -> bool {
        false
    }
    fn split_and_insert(&mut self, new_page_ptr: Ptr, key: K, value: V) -> ~SimpleBlinkLeaf<K, V, Ptr> {
        assert!(self.can_contain(&key));

        let new_size = self.values.len()/2;
        let idx = algorithm::bsearch_idx(self.keys, &key);

        self.values.insert(idx, value);
        self.keys.insert(idx, key);

        let values_new = utils::split_at(&mut self.values, new_size);
        let keys_new = utils::split_at(&mut self.keys, new_size);

        let right = self.right.take();
        self.right = Some(new_page_ptr.clone());
        ~SimpleBlinkLeaf {
            my_ptr: new_page_ptr,
            keys: keys_new,
            values: values_new,
            right: self.right.clone()
        }
    }

    fn insert(&mut self, key: K, value: V) { fail!() }

    fn max_key<'a>(&'a self) -> &'a K { fail!() }
    fn min_key<'a>(&'a self) -> &'a K { fail!() }
}

