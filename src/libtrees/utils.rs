use std::vec;


pub fn split_at<T>(vec: &mut~[T], i: uint) ->  ~[T] {
    let len = vec.len();
    assert!(i < len);
    let mut vec_new = vec::with_capacity(len - i);
    let mut j = len;
    while j > i {
        vec_new.push(vec.pop());
    }
    vec_new
}
