use crate::error::{Error, Result};
use crate::join::estimate::Estimate;
use crate::join::graph::{Edge, EdgeID, Graph, VertexID, VertexSet};
use crate::join::reorder::{JoinEdge, Reorder, Tree};
use crate::join::{JoinKind, JoinOp};
use crate::lgc::{Op, OpKind};
use smallvec::{smallvec, SmallVec};
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

/// DPhyp is an improved DP algorithm for join reorder, annouced
/// by paper "Dynamic Programming Strikes Back".
/// It uses hyperedge to model non-inner join, and provide efficient
/// enumeration on pairs of connected subgraph(csg) and connected
/// complement(cmp).
pub struct DPhyp<E>(E);

impl<E: Estimate> DPhyp<E> {
    #[inline]
    pub fn new(est: E) -> Self {
        DPhyp(est)
    }

    /// The method tries to find all neighbors of given csg, setup each csg-cmp-pair,
    /// then emit the pair to build join tree.
    /// The invoking order of this method requires from bigest csg to smallest csg.
    /// The search of neighbors will exclude any csg smaller than current one,
    /// so we avoid duplicates of csg-cmp-pairs.
    /// e.g. suppose we have csg [R1], [R2], and edge [R1] <=> [R2].
    /// if emit_csg([R2]) is invoked, there is no pair found as R1 smaller than R2.
    /// if emit_csg([R1]) is invoked, one pair [[R1], [R2]] will bit emitted.
    fn emit_csg<'a>(
        &mut self,
        graph: &'a Graph,
        best_plans: &mut BestPlans<'a>,
        vset: VertexSet,
    ) -> Result<()> {
        let exclude = vset | graph.vset().lt_vset(vset.min_id().unwrap());
        for nb_edge in neighbors(graph, vset, exclude) {
            if nb_edge.single {
                // single vertex, ensure connectivity
                // identify left and right sides
                let (l_vset, r_vset) = if nb_edge.left {
                    (VertexSet::from(nb_edge.vid), vset)
                } else {
                    (vset, VertexSet::from(nb_edge.vid))
                };
                if !skip_csg_cmp(best_plans, l_vset, r_vset) {
                    self.emit_csg_cmp(graph, best_plans, l_vset, r_vset, graph.edge(nb_edge.eid))?;
                }
            }
            self.enumer_cmp(
                graph,
                best_plans,
                vset,
                VertexSet::from(nb_edge.vid),
                exclude,
            )?;
        }
        Ok(())
    }

    /// Emit a given csg-cmp-pair, compute the cost,
    /// and merge into the DP table.
    fn emit_csg_cmp<'a>(
        &mut self,
        graph: &'a Graph,
        best_plans: &mut BestPlans<'a>,
        l_vset: VertexSet,
        r_vset: VertexSet,
        edge: &'a Edge,
    ) -> Result<()> {
        let mut edge = Cow::Borrowed(edge);
        // merge all available edges
        if edge.kind == JoinKind::Inner {
            for merge_her in graph.hyper_edge_refs() {
                let merge_edge = graph.edge(merge_her.eid);
                // Only check edge of inner join, and exclude same edge.
                // Then check eligibility set matches.
                if merge_edge.kind == JoinKind::Inner
                    && merge_edge.e_vset != edge.e_vset
                    && ((l_vset.includes(merge_her.l_vset) && r_vset.includes(merge_her.r_vset))
                        || (l_vset.includes(merge_her.r_vset) && r_vset.includes(merge_her.l_vset)))
                {
                    let edge = edge.to_mut();
                    edge.cond.extend_from_slice(&merge_edge.cond);
                    edge.e_vset |= merge_edge.e_vset;
                }
            }
        }
        let rows = self.0.estimate_join_rows(graph, l_vset, r_vset, &edge)?;
        let cost = match (best_plans.get(&l_vset), best_plans.get(&r_vset)) {
            (Some(l_ent), Some(r_ent)) => l_ent.tree.cost() + r_ent.tree.cost() + rows,
            _ => return Err(Error::InvalidJoinTransformation),
        };
        // check if it's better than existing one and update
        let (k, sk) = plan_key(l_vset, r_vset);
        match best_plans.entry(k) {
            Entry::Vacant(vac) => {
                let join_edge = JoinEdge {
                    l_vset,
                    r_vset,
                    edge,
                    rows,
                    cost,
                };
                vac.insert(PlanEntry {
                    tree: Tree::Join(join_edge),
                    skips: smallvec![sk],
                });
            }
            Entry::Occupied(mut occ) => {
                if occ.get().tree.cost() > cost {
                    let join_edge = JoinEdge {
                        l_vset,
                        r_vset,
                        edge,
                        rows,
                        cost,
                    };
                    let occ = occ.get_mut();
                    occ.tree = Tree::Join(join_edge);
                    occ.skips.push(sk);
                } else {
                    let occ = occ.get_mut();
                    occ.skips.push(sk);
                }
            }
        }
        Ok(())
    }

    /// The method is called for each connected subgraph.
    /// All neighbors will be examined and try to extend csg.
    /// If extension succeeds, trigger the search of csg-cmp-pairs.
    fn enumer_csg<'a>(
        &mut self,
        graph: &'a Graph,
        best_plans: &mut BestPlans<'a>,
        s1: VertexSet,
        exclude: VertexSet,
    ) -> Result<()> {
        let nbs = neighbors(graph, s1, exclude);
        for nb_edge in &nbs {
            let vset = s1 | nb_edge.vid;
            if best_plans.contains_key(&vset) {
                self.emit_csg(graph, best_plans, vset)?;
            }
        }

        let new_ex = {
            let mut ex = exclude;
            for nb_edge in &nbs {
                ex |= nb_edge.vid;
            }
            ex
        };
        for nb_edge in nbs {
            self.enumer_csg(graph, best_plans, s1 | nb_edge.vid, new_ex)?;
        }
        Ok(())
    }

    /// This method extends cmp with its neighbors, to find csg-cmp-pair and emit.
    /// Then recursively call itself with extended cmp.
    fn enumer_cmp<'a>(
        &mut self,
        graph: &'a Graph,
        best_plans: &mut BestPlans<'a>,
        s1: VertexSet,
        s2: VertexSet,
        mut exclude: VertexSet,
    ) -> Result<()> {
        let nbs = neighbors(graph, s2, exclude);
        for nb_edge in &nbs {
            let vs = s2 | VertexSet::from(nb_edge.vid);
            if best_plans.contains_key(&vs)
                && let Some((l_vset, r_vset, edge)) =
                    graph.hyper_edge_refs().iter().find_map(|her| {
                        if s1.includes(her.l_vset) && vs.includes(her.r_vset) {
                            Some((s1, vs, graph.edge(her.eid)))
                        } else if s1.includes(her.r_vset) && vs.includes(her.l_vset) {
                            Some((vs, s1, graph.edge(her.eid)))
                        } else {
                            None
                        }
                    })
                    && !skip_csg_cmp(best_plans, l_vset, r_vset) {
                        self.emit_csg_cmp(graph, best_plans, l_vset, r_vset, edge)?;
                    }
        }
        // Recursive call with extended s2
        for nb_edge in &nbs {
            exclude |= nb_edge.vid
        }
        for nb_edge in nbs {
            self.enumer_cmp(
                graph,
                best_plans,
                s1,
                s2 | VertexSet::from(nb_edge.vid),
                exclude,
            )?;
        }
        Ok(())
    }
}

impl<E: Estimate> Reorder for DPhyp<E> {
    #[inline]
    fn before(&mut self, graph: &mut Graph) -> Result<()> {
        graph.reset_hyper_edge_refs();
        Ok(())
    }

    #[inline]
    fn reorder(mut self, graph: &Graph) -> Result<Op> {
        if graph.hyper_edge_refs().is_empty() {
            return Err(Error::HyperedgesNotInitialized);
        }
        let mut best_plans = HashMap::new();
        // initialize each single table
        for vid in graph.vids() {
            let vset = VertexSet::from(vid);
            let rows = self.0.estimate_qry_rows(vset)?;
            best_plans.insert(
                vset,
                PlanEntry {
                    tree: Tree::Single { rows, cost: rows },
                    skips: smallvec![],
                },
            );
        }
        // iterate over vertex in reverse order
        for vid in graph.rev_vids() {
            self.emit_csg(graph, &mut best_plans, VertexSet::from(vid))?;
            let exclude = graph.vset().lt_vset(vid);
            self.enumer_csg(graph, &mut best_plans, VertexSet::from(vid), exclude)?;
        }
        // finally build the join tree from optimal plans.
        let vset = graph.vset();
        let tree = &best_plans[&vset];
        build_join_tree(graph, &best_plans, vset, tree)
    }
}

/// Neighborhood, namely N, is defined in paper that it is a set of hypernodes,
/// connected to a given hypernode, namely S, and do not intersect either S or certain
/// excluding set, namely X.
/// The neighbors are represented by minimal vertex IDs. e.g. suppose we have a hyperedge
/// [R1, R2, R3] <=> [R4, R5, R6]. The neighbor of [R1, R2, R3] is [R4, R5, R6], and
/// we use R4 to represent it.
fn neighbors(graph: &Graph, vset: VertexSet, exclude: VertexSet) -> SmallVec<[NbEdge; 2]> {
    let mut res = smallvec![];
    for her in graph.hyper_edge_refs() {
        if vset.includes(her.l_vset) && !vset.includes(her.r_vset) && !exclude.contains(her.r_vid) {
            res.push(NbEdge {
                vid: her.r_vid,
                eid: her.eid,
                single: her.r_vset.len() == 1,
                left: false,
            })
        } else if vset.includes(her.r_vset)
            && !vset.includes(her.l_vset)
            && !exclude.contains(her.l_vid)
        {
            res.push(NbEdge {
                vid: her.l_vid,
                eid: her.eid,
                single: her.l_vset.len() == 1,
                left: true,
            })
        }
    }
    res
}

struct NbEdge {
    vid: VertexID,
    eid: EdgeID,
    single: bool,
    left: bool,
}

fn build_join_tree(
    graph: &Graph,
    best_plans: &HashMap<VertexSet, PlanEntry>,
    vset: VertexSet,
    ent: &PlanEntry,
) -> Result<Op> {
    match &ent.tree {
        Tree::Single { .. } => {
            let vid = vset.single().unwrap();
            let qid = graph.vid_to_qid(vid)?;
            Ok(Op::new(OpKind::Query(qid)))
        }
        Tree::Join(JoinEdge {
            l_vset,
            r_vset,
            edge,
            ..
        }) => {
            let l_tree = &best_plans[l_vset];
            let left = build_join_tree(graph, best_plans, *l_vset, l_tree)?;
            let r_tree = &best_plans[r_vset];
            let right = build_join_tree(graph, best_plans, *r_vset, r_tree)?;
            let cond: Vec<_> = graph.preds(edge.cond.clone()).cloned().collect();
            let filt: Vec<_> = graph.preds(edge.filt.clone()).cloned().collect();
            let op = Op::new(OpKind::qualified_join(
                edge.kind,
                JoinOp::try_from(left)?,
                JoinOp::try_from(right)?,
                cond,
                filt,
            ));
            Ok(op)
        }
    }
}

/// Plan entry adds skip field to avoid duplicated computations of
/// identical joins. e.g. there might be multiple edges in clique join
/// between two sides. With skip check, we only need to merge these
/// edges and estimate cost once.
struct PlanEntry<'a> {
    skips: SmallVec<[VertexSet; 4]>,
    tree: Tree<'a>,
}

type BestPlans<'a> = HashMap<VertexSet, PlanEntry<'a>>;

#[inline]
fn skip_csg_cmp(best_plans: &BestPlans, l_vset: VertexSet, r_vset: VertexSet) -> bool {
    let (k, sk) = plan_key(l_vset, r_vset);
    if let Some(ent) = best_plans.get(&k) {
        return ent.skips.contains(&sk);
    }
    false
}

// we use small vset as skip key
#[inline]
fn plan_key(l_vset: VertexSet, r_vset: VertexSet) -> (VertexSet, VertexSet) {
    let vset = l_vset | r_vset;
    let skip_vset = if l_vset < r_vset { l_vset } else { r_vset };
    (vset, skip_vset)
}
