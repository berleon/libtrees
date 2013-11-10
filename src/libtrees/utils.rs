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


use std::vec;


pub fn split_at<T>(vec: &mut~[T], i: uint) ->  ~[T] {
    let len = vec.len();
    if len == 0 { return ~[] }
    assert!(i < len);
    let mut vec_new = vec::with_capacity(len - i);
    let mut j = len;
    while j > i {
        vec_new.unshift(vec.pop());
        j -= 1;
    }
    vec_new
}

#[cfg(test)]
mod test {
    use super::split_at;
    #[test]
    fn test_split_at() {
        let mut vec : ~[uint] = ~[];
        assert!(split_at(&mut vec, 0) == ~[]);
        assert!(vec == ~[]);

        let mut vec = ~[0];
        assert!(split_at(&mut vec, 0) == ~[0]);
        assert!(vec == ~[]);


        let mut vec = ~[1,2,3,4,5];
        let expected = vec.clone();
        let rest = split_at(&mut vec, 0);
        assert!(rest == expected);
        assert!(vec == ~[]);

    }
}
