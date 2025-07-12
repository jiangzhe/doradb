use crate::error::{Error, Result};
use crate::join::JoinKind;
use crate::lgc::{Op, OpKind};
use doradb_expr::{ExprKind, QueryID};
use indexmap::IndexMap;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, Deref, DerefMut};

// Support at most 31 tables in single join graph.
// The threshold is actually very high for DP algorithm
// used in join reorder.
// If number of tables to join is more than 20,
// it will be very slow to determine the correct join order.
pub const MAX_JOIN_QUERIES: usize = 31;

/// Graph is a container maintaining two or more queries
/// to be joined together, with a set of join types and
/// join conditions.
#[derive(Debug, Clone, Default)]
pub struct Graph {
    vs: VertexSet,
    v2q: HashMap<VertexID, QueryID>,
    q2v: HashMap<QueryID, VertexID>,
    edge_map: IndexMap<VertexSet, EdgeIDs>,
    qs: Vec<Op>,
    edge_arena: Arena<Edge>,
    pred_arena: Arena<ExprKind>,
    hyp_edge_refs: Option<Vec<HyperEdgeRef>>,
}

impl Graph {
    #[inline]
    pub fn add_qry(&mut self, qry_id: QueryID) -> Result<VertexID> {
        let v_idx = self.qs.len();
        if v_idx >= MAX_JOIN_QUERIES {
            return Err(Error::TooManyTablesToJoin);
        }
        let vid = VertexID(1u32 << v_idx);
        self.vs |= vid;
        self.v2q.insert(vid, qry_id);
        self.q2v.insert(qry_id, vid);
        self.qs.push(Op::new(OpKind::Query(qry_id)));
        Ok(vid)
    }

    #[inline]
    pub fn queries(&self) -> Vec<QueryID> {
        self.qs
            .iter()
            .filter_map(|op| match &op.kind {
                OpKind::Query(qry_id) => Some(*qry_id),
                _ => None,
            })
            .collect()
    }

    #[inline]
    pub fn n_vertexes(&self) -> usize {
        self.vs.len()
    }

    #[inline]
    pub fn vids(&self) -> impl Iterator<Item = VertexID> + use<> {
        self.vs.into_iter()
    }

    #[inline]
    pub fn rev_vids(&self) -> impl Iterator<Item = VertexID> + use<> {
        self.vs.rev_iter()
    }

    #[inline]
    pub fn vset(&self) -> VertexSet {
        self.vs
    }

    #[inline]
    pub fn children(&self) -> SmallVec<[&Op; 2]> {
        self.qs.iter().collect()
    }

    #[inline]
    pub fn children_mut(&mut self) -> SmallVec<[&mut Op; 2]> {
        self.qs.iter_mut().collect()
    }

    #[inline]
    pub fn exprs(&self) -> impl IntoIterator<Item = &ExprKind> {
        self.pred_arena.iter()
    }

    #[inline]
    pub fn exprs_mut(&mut self) -> impl IntoIterator<Item = &mut ExprKind> {
        self.pred_arena.iter_mut()
    }

    #[inline]
    pub fn n_edges(&self) -> usize {
        self.edge_arena.len()
    }

    #[inline]
    pub fn eids(&self) -> impl Iterator<Item = EdgeID> + use<> {
        (0..self.edge_arena.len()).map(|u| EdgeID(u as u16))
    }

    #[inline]
    pub fn vset_eids(&self) -> impl Iterator<Item = (&VertexSet, &EdgeIDs)> + '_ {
        self.edge_map.iter()
    }

    #[inline]
    pub fn edge(&self, eid: EdgeID) -> &Edge {
        &self.edge_arena[eid.0 as usize]
    }

    #[inline]
    pub fn pred(&self, pid: PredID) -> &ExprKind {
        &self.pred_arena[pid.0 as usize]
    }

    #[inline]
    pub fn preds(&self, pids: PredIDs) -> impl Iterator<Item = &ExprKind> {
        pids.into_iter().map(|pid| self.pred(pid))
    }

    #[inline]
    pub fn eids_by_vset(&self, vset: VertexSet) -> Option<EdgeIDs> {
        self.edge_map.get(&vset).cloned()
    }

    #[inline]
    pub fn reset_hyper_edge_refs(&mut self) {
        let mut refs = Vec::with_capacity(self.n_edges());
        for eid in self.eids() {
            let edge = self.edge(eid);
            // currently, we do not support freely transform hyper edge.
            let l_vset = edge.e_vset & edge.l_vset;
            let l_vid = l_vset.min_id().unwrap();
            let r_vset = edge.e_vset & edge.r_vset;
            let r_vid = r_vset.min_id().unwrap();
            refs.push(HyperEdgeRef {
                eid,
                l_vset,
                l_vid,
                r_vset,
                r_vid,
            })
        }
        self.hyp_edge_refs = Some(refs)
    }

    #[inline]
    pub fn hyper_edge_refs(&self) -> &[HyperEdgeRef] {
        if let Some(refs) = self.hyp_edge_refs.as_ref() {
            refs
        } else {
            &[]
        }
    }

    /// Add one edge to join graph.
    /// The arguments l_vset and r_vset are original tables specified on
    /// left and right side of this join edge.
    /// They are super set of eligibility set this join requires.
    #[inline]
    pub fn add_edge(
        &mut self,
        kind: JoinKind,
        l_vset: VertexSet,
        r_vset: VertexSet,
        e_vset: VertexSet,
        cond: Vec<ExprKind>,
        filt: Vec<ExprKind>,
    ) {
        let Graph {
            edge_map,
            edge_arena,
            pred_arena,
            ..
        } = self;
        let eids = edge_map.entry(l_vset | r_vset).or_default();
        // only inner join could be separated to multiple edges upon same join tree.
        assert!(eids
            .iter()
            .all(|eid| edge_arena[eid.0 as usize].kind == JoinKind::Inner));
        // compact edge and expression
        let cond = {
            let mut ids = PredIDs::new();
            for c in cond {
                let id = pred_arena.insert(c);
                ids.push(PredID(id));
            }
            ids
        };
        let filt = {
            let mut ids = PredIDs::new();
            for c in filt {
                let id = pred_arena.insert(c);
                ids.push(PredID(id));
            }
            ids
        };
        let edge = Edge {
            kind,
            l_vset,
            r_vset,
            e_vset,
            cond,
            filt,
        };
        let eid = self.edge_arena.insert(edge);
        eids.push(EdgeID(eid));
    }

    #[inline]
    pub fn qids_to_vset<'a, I>(&self, qry_ids: I) -> Result<VertexSet>
    where
        I: IntoIterator<Item = &'a QueryID>,
    {
        let mut vset = VertexSet::default();
        for qry_id in qry_ids {
            if let Some(vid) = self.q2v.get(qry_id) {
                vset |= *vid;
            } else {
                return Err(Error::QueryNotFound(*qry_id));
            }
        }
        Ok(vset)
    }

    #[inline]
    pub(crate) fn vid_to_qid(&self, vid: VertexID) -> Result<QueryID> {
        self.v2q
            .get(&vid)
            .cloned()
            .ok_or(Error::InvalidJoinVertexSet)
    }
}

#[allow(dead_code)]
#[inline]
pub(crate) fn qid_to_vid(map: &HashMap<QueryID, VertexID>, qid: QueryID) -> Result<VertexID> {
    map.get(&qid).cloned().ok_or(Error::QueryNotFound(qid))
}

#[allow(dead_code)]
#[inline]
pub(crate) fn vset_to_qids<C>(map: &HashMap<VertexID, QueryID>, vset: VertexSet) -> Result<C>
where
    C: FromIterator<QueryID>,
{
    if let Some(vid) = vset.single() {
        map.get(&vid)
            .cloned()
            .ok_or(Error::InvalidJoinVertexSet)
            .map(|qid| std::iter::once(qid).collect::<C>())
    } else {
        vset.into_iter()
            .map(|vid| map.get(&vid).cloned().ok_or(Error::InvalidJoinVertexSet))
            .collect::<Result<C>>()
    }
}

/// Edge of join graph.
/// This is the "hyperedge" introduced in paper
/// "Dynamic Programming Strikes Back".
/// Field `l_vset` contains all vertexes at left side.
/// Field `r_vset` contains all vertexes at right side.
/// Field `e_vset` is join eligibility set, which contains
/// all required vertexes to perform this join.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Edge {
    pub kind: JoinKind,
    // All nodes on left side.
    pub l_vset: VertexSet,
    // All nodes on right side.
    pub r_vset: VertexSet,
    // Eligibility set, to validate two table sets can be
    // joined or not.
    pub e_vset: VertexSet,
    // Join conditions that should be evaluated in join.
    // They are conjunctive.
    pub cond: SmallVec<[PredID; 8]>,
    // Filt which should be applied after the join.
    // Inner join will always has this field empty because
    // all filters can be evaluated as join condition in join phase.
    // They are conjunctive.
    pub filt: SmallVec<[PredID; 8]>,
}

#[derive(Debug, Clone, Default)]
pub struct HyperEdgeRef {
    pub eid: EdgeID,
    pub l_vset: VertexSet,
    // minimal vertex id of left side
    pub l_vid: VertexID,
    pub r_vset: VertexSet,
    // minimal vertex id of right side
    pub r_vid: VertexID,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VertexSet {
    // Use bitmap to encode query id as vertexes in the graph
    bits: u32,
}

impl VertexSet {
    #[inline]
    pub fn includes(&self, other: Self) -> bool {
        self.bits & other.bits == other.bits
    }

    #[inline]
    pub fn intersects(&self, other: Self) -> bool {
        self.bits & other.bits != 0
    }

    #[inline]
    pub fn contains(&self, vid: VertexID) -> bool {
        self.bits & vid.0 != 0
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bits == 0
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.bits.count_ones() as usize
    }

    /// fast path to get vid if set only contains one element
    #[inline]
    pub fn single(&self) -> Option<VertexID> {
        if self.len() == 1 {
            Some(VertexID(self.bits))
        } else {
            None
        }
    }

    #[inline]
    pub fn min_id(&self) -> Option<VertexID> {
        if self.is_empty() {
            None
        } else {
            Some(VertexID(1u32 << self.bits.trailing_zeros()))
        }
    }

    /// returns set of vertexes less than given vid.
    /// As vertex set is represented as bitset, we can
    /// use bit mask to speed the operation.
    #[inline]
    pub fn lt_vset(&self, vid: VertexID) -> Self {
        VertexSet {
            bits: self.bits & (vid.0 - 1),
        }
    }

    /// Returns an iterator of vertex ID in reverse order.
    #[inline]
    pub fn rev_iter(self) -> VertexRevIter {
        let (value, count) = if self.is_empty() {
            (0, 0)
        } else {
            let value = 1 << (31 - self.bits.leading_zeros());
            let count = self.bits.count_ones();
            (value, count)
        };
        VertexRevIter {
            bits: self.bits,
            value,
            count,
        }
    }
}

impl IntoIterator for VertexSet {
    type Item = VertexID;
    type IntoIter = VertexIter;
    #[inline]
    fn into_iter(self) -> VertexIter {
        VertexIter {
            bits: self.bits,
            value: 1,
            count: self.bits.count_ones(),
        }
    }
}

impl FromIterator<VertexSet> for VertexSet {
    #[inline]
    fn from_iter<T: IntoIterator<Item = VertexSet>>(iter: T) -> Self {
        let mut res = VertexSet::default();
        for it in iter {
            res |= it;
        }
        res
    }
}

impl From<VertexID> for VertexSet {
    #[inline]
    fn from(src: VertexID) -> Self {
        VertexSet { bits: src.0 }
    }
}

pub struct VertexIter {
    bits: u32,
    value: u32,
    count: u32,
}

impl Iterator for VertexIter {
    type Item = VertexID;
    #[inline]
    fn next(&mut self) -> Option<VertexID> {
        if self.count == 0 {
            None
        } else {
            loop {
                if self.bits & self.value != 0 {
                    self.count -= 1;
                    let vid = VertexID(self.value);
                    self.value <<= 1;
                    return Some(vid);
                } else {
                    self.value <<= 1;
                }
            }
        }
    }
}

pub struct VertexRevIter {
    bits: u32,
    value: u32,
    count: u32,
}

impl Iterator for VertexRevIter {
    type Item = VertexID;
    #[inline]
    fn next(&mut self) -> Option<VertexID> {
        if self.count == 0 {
            None
        } else {
            loop {
                if self.bits & self.value != 0 {
                    self.count -= 1;
                    let vid = VertexID(self.value);
                    self.value >>= 1;
                    return Some(vid);
                } else {
                    self.value >>= 1;
                }
            }
        }
    }
}

impl BitOrAssign<VertexID> for VertexSet {
    #[inline]
    fn bitor_assign(&mut self, rhs: VertexID) {
        self.bits |= rhs.0;
    }
}

impl BitOrAssign for VertexSet {
    #[inline]
    fn bitor_assign(&mut self, rhs: Self) {
        self.bits |= rhs.bits;
    }
}

impl BitOr for VertexSet {
    type Output = Self;
    #[inline]
    fn bitor(self, rhs: Self) -> Self {
        VertexSet {
            bits: self.bits | rhs.bits,
        }
    }
}

impl BitOr<VertexID> for VertexSet {
    type Output = Self;
    #[inline]
    fn bitor(self, rhs: VertexID) -> Self {
        VertexSet {
            bits: self.bits | rhs.0,
        }
    }
}

impl BitAnd for VertexSet {
    type Output = Self;
    #[inline]
    fn bitand(self, rhs: Self) -> Self {
        VertexSet {
            bits: self.bits & rhs.bits,
        }
    }
}

impl BitAndAssign for VertexSet {
    #[inline]
    fn bitand_assign(&mut self, rhs: Self) {
        self.bits &= rhs.bits
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct VertexID(pub(crate) u32);

/// The numeric identifier of edges.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeID(pub(crate) u16);

/// As we use union feature of SmallVec, we have 16 free bytes with inline format.
/// That allows to store 8 u16's.
pub type EdgeIDs = SmallVec<[EdgeID; 8]>;

/// Two pointers are allowed to store with inline format.
pub type EdgeRefs<'a> = SmallVec<[&'a Edge; 2]>;

/// The numeric identifier of predicates in join condition and filter.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PredID(pub(crate) u16);

pub type PredIDs = SmallVec<[PredID; 8]>;

/// Simple arena backed by Vec to store objects without deletion.
#[derive(Debug, Clone, Default)]
pub(crate) struct Arena<T> {
    inner: Vec<T>,
}

impl<T> Arena<T> {
    #[inline]
    fn insert(&mut self, value: T) -> u16 {
        let idx = self.inner.len();
        assert!(idx <= u16::MAX as usize);
        self.inner.push(value);
        idx as u16
    }
}

impl<T> Deref for Arena<T> {
    type Target = [T];
    #[inline]
    fn deref(&self) -> &[T] {
        &self.inner
    }
}

impl<T> DerefMut for Arena<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_size_of_smallvec_in_compact_graph() {
        use std::mem::size_of;
        println!(
            "size of SmallVec<[EdgeID;8]> is {}",
            size_of::<SmallVec<[EdgeID; 8]>>()
        );
        println!(
            "size of SmallVec<[PredID;8]> is {}",
            size_of::<SmallVec<[PredID; 8]>>()
        );
    }

    #[test]
    fn test_vertex_set_rev_iter() {
        let vset = VertexSet { bits: 6 };
        assert_eq!(
            vec![VertexID(4), VertexID(2)],
            vset.rev_iter().collect::<Vec<_>>()
        );
        let vset = VertexSet { bits: 0 };
        assert_eq!(Vec::<VertexID>::new(), vset.rev_iter().collect::<Vec<_>>());
    }
}
