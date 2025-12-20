use std::ops::{Index, IndexMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeIndex(u32);

impl NodeIndex {
    #[inline]
    fn val(self) -> usize {
        self.0 as usize
    }

    #[inline]
    fn new(val: usize) -> Self {
        NodeIndex(val as u32)
    }
}

const NIDX_NONE: NodeIndex = NodeIndex(!0);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeIndex(u32);

impl EdgeIndex {
    #[inline]
    fn val(self) -> usize {
        self.0 as usize
    }

    #[inline]
    fn new(val: usize) -> Self {
        EdgeIndex(val as u32)
    }
}

const EIDX_NONE: EdgeIndex = EdgeIndex(!0);

pub struct Node<N> {
    // First edge that enters this node
    entrance: EdgeIndex,
    // First edge that exits this node
    exit: EdgeIndex,
    // Payload of the node
    data: N,
}

#[allow(dead_code)]
pub struct Edge<E> {
    // Index of start node
    start: NodeIndex,
    // Index of end node
    end: NodeIndex,
    // Payload of the edge
    data: E,
    // Index of next edge, which shares same start
    start_next: EdgeIndex,
    // Index of next edge, which shares same end
    end_next: EdgeIndex,
}

pub struct DiGraph<N, E> {
    nodes: Vec<Node<N>>,
    edges: Vec<Edge<E>>,
}

impl<N, E> DiGraph<N, E> {
    #[inline]
    pub fn new() -> Self {
        DiGraph {
            nodes: vec![],
            edges: vec![],
        }
    }

    #[inline]
    pub fn with_capacity(nodes: usize, edges: usize) -> Self {
        DiGraph {
            nodes: Vec::with_capacity(nodes),
            edges: Vec::with_capacity(edges),
        }
    }

    #[inline]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    #[inline]
    pub fn add_node(&mut self, data: N) -> NodeIndex {
        assert!(self.nodes.len() < NIDX_NONE.val());
        let idx = NodeIndex::new(self.nodes.len());
        let node = Node {
            entrance: EIDX_NONE,
            exit: EIDX_NONE,
            data,
        };
        self.nodes.push(node);
        idx
    }

    #[inline]
    pub fn node(&self, idx: NodeIndex) -> Option<&N> {
        self.nodes.get(idx.val()).map(|n| &n.data)
    }

    #[inline]
    pub fn node_mut(&mut self, idx: NodeIndex) -> Option<&mut N> {
        self.nodes.get_mut(idx.val()).map(|n| &mut n.data)
    }

    #[inline]
    pub fn add_edge(&mut self, start: NodeIndex, end: NodeIndex, data: E) -> EdgeIndex {
        assert!(
            self.edges.len() < EIDX_NONE.val()
                && start.val() < self.node_count()
                && end.val() < self.node_count()
                && start != end
        );
        let idx = EdgeIndex::new(self.edges.len());
        let mut edge = Edge {
            start,
            end,
            data,
            start_next: EIDX_NONE,
            end_next: EIDX_NONE,
        };
        // Safety:
        //
        // Invariant is ensured by above assertion.
        let (sn, en) = unsafe { self.raw_nodes2_mut(start.val(), end.val()) };
        edge.start_next = sn.exit;
        edge.end_next = en.entrance;
        sn.exit = idx;
        en.entrance = idx;
        self.edges.push(edge);
        idx
    }

    #[inline]
    pub fn edge(&self, idx: EdgeIndex) -> Option<&E> {
        self.edges.get(idx.val()).map(|e| &e.data)
    }

    #[inline]
    pub fn edge_mut(&mut self, idx: EdgeIndex) -> Option<&mut E> {
        self.edges.get_mut(idx.val()).map(|e| &mut e.data)
    }

    #[inline]
    pub fn merge_edge(&mut self, start: NodeIndex, end: NodeIndex, data: E) -> EdgeIndex {
        if let Some(idx) = self.find_edge(start, end) {
            self[idx] = data;
            return idx;
        }
        self.add_edge(start, end, data)
    }

    #[inline]
    pub fn find_edge(&self, start: NodeIndex, end: NodeIndex) -> Option<EdgeIndex> {
        match self.nodes.get(start.val()) {
            None => None,
            Some(node) => self.find_exit_edge(node, end),
        }
    }

    #[inline]
    fn find_exit_edge(&self, node: &Node<N>, end: NodeIndex) -> Option<EdgeIndex> {
        let mut idx = node.exit;
        while let Some(edge) = self.edges.get(idx.val()) {
            if edge.end == end {
                return Some(idx);
            }
            idx = edge.start_next;
        }
        None
    }

    /// # Safety
    ///
    /// Caller must ensure idx1 and idx2 are within bound and different
    #[inline]
    unsafe fn raw_nodes2_mut(&mut self, idx1: usize, idx2: usize) -> (&mut Node<N>, &mut Node<N>) {
        let ptr = self.nodes.as_mut_ptr();
        let n1 = unsafe { &mut *ptr.add(idx1) };
        let n2 = unsafe { &mut *ptr.add(idx2) };
        (n1, n2)
    }
}

impl<N, E> Default for DiGraph<N, E> {
    #[inline]
    fn default() -> Self {
        DiGraph::new()
    }
}

impl<N, E> Index<NodeIndex> for DiGraph<N, E> {
    type Output = N;
    #[inline]
    fn index(&self, index: NodeIndex) -> &N {
        &self.nodes[index.val()].data
    }
}

impl<N, E> IndexMut<NodeIndex> for DiGraph<N, E> {
    #[inline]
    fn index_mut(&mut self, index: NodeIndex) -> &mut N {
        &mut self.nodes[index.val()].data
    }
}

impl<N, E> Index<EdgeIndex> for DiGraph<N, E> {
    type Output = E;
    #[inline]
    fn index(&self, index: EdgeIndex) -> &E {
        &self.edges[index.val()].data
    }
}

impl<N, E> IndexMut<EdgeIndex> for DiGraph<N, E> {
    #[inline]
    fn index_mut(&mut self, index: EdgeIndex) -> &mut E {
        &mut self.edges[index.val()].data
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_digraph1() {
        let mut g = DiGraph::default();
        let one = g.add_node(1);
        let two = g.add_node(2);
        let edge = g.add_edge(one, two, "1=>2");
        assert_eq!(2, g.node_count());
        assert_eq!(1, g.edge_count());
        assert_eq!(1, g[one]);
        assert_eq!(2, g[two]);
        assert_eq!("1=>2", g[edge]);
        assert_eq!(edge, g.merge_edge(one, two, "?"));
        assert_eq!("?", g[edge]);
        g[edge] = "1=>2";
        assert_eq!("1=>2", g[edge]);
    }

    #[test]
    fn test_digraph2() {
        let mut g = DiGraph::with_capacity(16, 16);
        let one = g.add_node(1);
        let two = g.add_node(2);
        let three = g.add_node(3);
        let four = g.add_node(4);
        let e1 = g.add_edge(one, two, "1=>2");
        let e2 = g.add_edge(one, three, "1=>3");
        let _ = g.add_edge(two, four, "2=>4");
        let _ = g.add_edge(three, four, "3=>4");
        assert_eq!(4, g.node_count());
        assert_eq!(4, g.edge_count());
        assert_eq!(Some(&1), g.node(one));
        assert_eq!(Some(&mut 2), g.node_mut(two));
        assert_eq!(Some(&"1=>2"), g.edge(e1));
        assert_eq!(Some(&mut "1=>3"), g.edge_mut(e2));
        g.merge_edge(one, two, "?");
        assert_eq!("?", g[e1]);
        g[four] = 42;
        assert_eq!(g[four], 42);
        assert!(g.find_edge(one, four).is_none())
    }
}
