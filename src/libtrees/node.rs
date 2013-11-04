

pub enum Node<I, L> {
    INode(I),
    Leaf(L)
}

impl <I,L> Node<I,L> {
    pub fn isLeaf(&self) -> bool {
        match self{
            &Leaf(*)  => true,
            &INode(*) => false
        }
    }
    pub fn isINode(&self) -> bool {
        match self{
            &INode(*) => true,
            &Leaf(*)  => false
        }
    }
    pub fn getLeaf<'a>(&'a self) -> &'a L {
        match self{
            &Leaf(ref l)  => l,
            &INode(*) => fail!("called getLeaf on an INode"),
        }
    }
    pub fn getINode<'a>(&'a self) -> &'a I {
        match self{
            &INode(ref i) => i,
            &Leaf(*)  => fail!("called getINode on a Leaf"),
        }
    }
}

