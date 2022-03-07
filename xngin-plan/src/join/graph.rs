use crate::error::{Error, Result};
use crate::join::JoinKind;
use crate::op::Op;
use indexmap::IndexMap;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign, Deref, DerefMut};
use xngin_expr::{Expr, QueryID};

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
    pub(crate) vertexes: VertexSet,
    pub(crate) vmap: HashMap<VertexID, QueryID>,
    pub(crate) rev_vmap: HashMap<QueryID, VertexID>,
    pub(crate) edge_map: IndexMap<VertexSet, EdgeIDs>,
    pub(crate) queries: Vec<Op>,
    pub(crate) edge_arena: Arena<Edge>,
    pub(crate) pred_arena: Arena<Expr>,
}

impl Graph {
    #[inline]
    pub fn add_qry(&mut self, qry_id: QueryID) -> Result<VertexID> {
        let v_idx = self.queries.len();
        if v_idx >= MAX_JOIN_QUERIES {
            return Err(Error::TooManyTablesToJoin);
        }
        let vid = VertexID(1u32 << v_idx);
        self.vertexes |= vid;
        self.vmap.insert(vid, qry_id);
        self.rev_vmap.insert(qry_id, vid);
        self.queries.push(Op::Query(qry_id));
        Ok(vid)
    }

    #[inline]
    pub fn queries(&self) -> Vec<QueryID> {
        self.queries
            .iter()
            .filter_map(|op| match op {
                Op::Query(qry_id) => Some(*qry_id),
                _ => None,
            })
            .collect()
    }

    #[inline]
    pub fn exprs(&self) -> impl IntoIterator<Item = &Expr> {
        self.pred_arena.iter()
    }

    #[inline]
    pub fn exprs_mut(&mut self) -> impl IntoIterator<Item = &mut Expr> {
        self.pred_arena.iter_mut()
    }

    #[inline]
    pub fn n_edges(&self) -> usize {
        self.edge_arena.len()
    }

    #[inline]
    pub fn eids(&self) -> impl Iterator<Item = EdgeID> {
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
    pub fn pred(&self, pid: PredID) -> &Expr {
        &self.pred_arena[pid.0 as usize]
    }

    #[inline]
    pub fn preds(&self, pids: PredIDs) -> impl Iterator<Item = &Expr> {
        pids.into_iter().map(|pid| self.pred(pid))
    }

    #[inline]
    pub fn eids_by_vset(&self, vset: VertexSet) -> Option<EdgeIDs> {
        self.edge_map.get(&vset).cloned()
    }

    #[inline]
    pub fn add_edge(
        &mut self,
        kind: JoinKind,
        l_vset: VertexSet,
        r_vset: VertexSet,
        e_vset: VertexSet,
        cond: Vec<Expr>,
        filt: Vec<Expr>,
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
            if let Some(vid) = self.rev_vmap.get(qry_id) {
                vset |= *vid;
            } else {
                return Err(Error::QueryNotFound(*qry_id));
            }
        }
        Ok(vset)
    }
}

#[allow(dead_code)]
#[inline]
pub(crate) fn qid_to_vid(map: &HashMap<QueryID, VertexID>, qid: QueryID) -> Result<VertexID> {
    map.get(&qid).cloned().ok_or(Error::QueryNotFound(qid))
}

#[inline]
pub(crate) fn vid_to_qid(map: &HashMap<VertexID, QueryID>, vid: VertexID) -> Result<QueryID> {
    map.get(&vid).cloned().ok_or(Error::InvalidJoinVertexSet)
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
    // pub cond: Vec<Expr>,
    pub cond: SmallVec<[PredID; 8]>,
    // Filt which should be applied after the join.
    // Inner join will always has this field empty because
    // all filters can be evaluated as join condition in join phase.
    // They are conjunctive.
    // pub filt: Vec<Expr>,
    pub filt: SmallVec<[PredID; 8]>,
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
    pub fn min(&self) -> Option<VertexID> {
        if self.is_empty() {
            None
        } else {
            Some(VertexID(1u32 << self.bits.trailing_zeros()))
        }
    }
}

impl IntoIterator for VertexSet {
    type Item = VertexID;
    type IntoIter = Iter;
    #[inline]
    fn into_iter(self) -> Iter {
        Iter {
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

pub struct Iter {
    bits: u32,
    value: u32,
    count: u32,
}

impl Iterator for Iter {
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
}
