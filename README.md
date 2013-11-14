
# libtrees

Implementation of concurrent, disk or memory based trees in Rust.

My concrete plans are:

* Lehmann and Yao concurrent B-Tree [(paper)](http://www.cs.cornell.edu/courses/CS4411/2009sp/blink.pdf)
* mutlidimensional R-Tree
* Kd-Tree

## B-Tree

The B-Tree implementation is working for the single thread case.
Currenty the B-tree doesn't get writen to harddisk but rather into a Hashmap in main memomry.
I will update the interface soon, add some fancy building patterns and implement a hard disk storage.






