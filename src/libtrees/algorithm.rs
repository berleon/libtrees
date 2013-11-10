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


pub fn bsearch_idx<V: TotalOrd>(vector: &[V], key: &V) -> uint {
    let len = vector.len();
    let mut imin = 0u;
    let mut imax = vector.len();
    if len == 0 {
        return 0;
    }
    while(imin < imax) {
        let imid = imin + (imax-imin) / 2;
        if key.cmp(&vector[imid]) == Greater {
            imin = imid + 1;
        } else {
            imax = imid;
        }
    }
    return imin;
}

// just here to measure the differences between Ord and TotalOrd
fn bsearch_idx_ord<V: Ord>(vector: &[V], key: &V) -> uint {
    let len = vector.len();
    let mut imin = 0u;
    let mut imax = vector.len();
    if len == 0 {
        return 0;
    }
    while(imin < imax) {
        let imid = imin + (imax-imin) / 2;
        if key >= &vector[imid] {
            imin = imid + 1;
        } else {
            imax = imid;
        }
    }
    return imin;
}


#[cfg(test)]
mod test {
    use std::rand::random;
    use super::bsearch_idx;
    use super::bsearch_idx_ord;

    use extra::sort::quick_sort3;
    use extra::test::BenchHarness;
    #[allow(unnecessary_allocation)]
    #[test]
    fn test_bsearch_idx() {
        //              0 1 2 3 4  5  6
        let example = ~[1,3,4,6,8,10,20];
        assert!(bsearch_idx(example, &1) == 0);
        assert!(bsearch_idx(example, &4) == 2);

        assert!(bsearch_idx(example, &5) == 3);
        assert!(bsearch_idx(example, &6) == 3);

        assert!(bsearch_idx(example, &12) == 6);

        assert!(bsearch_idx(~[], &1) == 0);
        assert!(bsearch_idx(~[20], &10) == 0);
        assert!(bsearch_idx(~[20], &20) == 0);
        assert!(bsearch_idx(~[20], &30) == 1);
    }

    #[bench]
    fn bench_bsearch(b: &mut BenchHarness) {
        let mut vec: ~[uint] = do range(0,1000).map |_| { random() }.to_owned_vec();
        quick_sort3(vec);
        do b.iter {
            bsearch_idx(vec, &random());
        }
    }

    #[bench]
    fn bench_bsearch_ord(b: &mut BenchHarness) {
        let mut vec: ~[uint] = do range(0,1000).map |_| { random() }.to_owned_vec();
        quick_sort3(vec);
        do b.iter {
            bsearch_idx_ord(vec, &random());
        }
    }
}
