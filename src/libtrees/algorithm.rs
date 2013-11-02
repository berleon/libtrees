
pub fn bsearch_idx<V:TotalOrd>(vector: &[V], key: &V) -> uint {
    let mut imin = 0u;
    let mut imax = vector.len();

    while(imin + 1 != imax) {
        let imid = imin + (imax-imin) / 2;
        if vector[imid].cmp(key) == Less {
            imin = imid;
        } else if key.cmp(&vector[imid]) == Less {
            imax = imid;
        } else {
            return (imid)
        }
    }
    return imin;
}

#[cfg(test)]
mod test {
    use super::bsearch_idx;
    #[test]
    fn test_bsearch_idx() {
        //          0 1 2 3 4  5  6
        let exp = ~[1,3,4,6,8,10,20];
        assert!(bsearch_idx(exp, &1) == 0);
        assert!(bsearch_idx(exp, &4) == 2);
        assert!(bsearch_idx(exp, &6) == 3);
        assert!(bsearch_idx(exp, &11) == 5);
    }
}
