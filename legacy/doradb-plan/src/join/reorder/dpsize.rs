use crate::error::{Error, Result};
use crate::join::estimate::Estimate;
use crate::join::graph::{Edge, EdgeID, Graph, VertexSet};
use crate::join::reorder::{JoinEdge, Reorder, Tree};
use crate::join::{JoinKind, JoinOp};
use crate::lgc::{Op, OpKind};
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem;

/// DPsize is a classic DP algorithm for join reorder.
/// Suppose there are N tables to be joined.
/// DPsize initialize each table as a single join set.
/// Then iterate the size from 2 to N, to compute all best plans
/// of given size, based on previous results.
/// At the end of the iteration, we get the best plan of size N.
///
/// Note: DP algorithm requires cost model to pick best plan on
/// same join set. Here we use simplest cost function that just
/// sums row counts of intermediate result sets, which can be
/// considered as "logical" cost.
pub struct DPsize<E>(E);

impl<E: Estimate> DPsize<E> {
    #[inline]
    pub fn new(est: E) -> Self {
        DPsize(est)
    }
}

impl<E: Estimate> Reorder for DPsize<E> {
    #[inline]
    fn reorder(mut self, graph: &Graph) -> Result<Op> {
        // we store n+1 sizes, and leave size=0 empty.
        let n_vertexes = graph.n_vertexes();
        let mut best_plans = vec![HashMap::new(); n_vertexes + 1];
        // initialize each single table
        for vid in graph.vids() {
            let vset = VertexSet::from(vid);
            let rows = self.0.estimate_qry_rows(vset)?;
            // In current version, we made simple cost function: cost is same as number
            // of processed rows.
            // So cost of a table scan is just row count of this table.
            // Cost of a binary join is the input rows of both children and its output rows.
            best_plans[1].insert(vset, Tree::Single { rows, cost: rows });
        }
        // initialize edges as pairs of id and ref.
        let edges: Vec<(EdgeID, &Edge)> = graph.eids().map(|eid| (eid, graph.edge(eid))).collect();
        // main loop to compute join size from 2 to N
        let mut target_plan = HashMap::new(); // target_plan is what we update in the iteration
        for join_size in 2..=n_vertexes {
            for l_size in 1..=n_vertexes / 2 {
                let r_size = join_size - l_size;
                // here we use a trick to swap the plan of target size with empty map,
                // so that we can perform update on it while looking up plans of smaller size.
                mem::swap(&mut target_plan, &mut best_plans[join_size]);
                // we only need to iterate over joined plans
                for (vi, ti) in &best_plans[l_size] {
                    for (vj, tj) in &best_plans[r_size] {
                        // If size is identical, we only iterate over small-large pairs.
                        // And we skip plans that has intersection.
                        if (l_size == r_size && vi >= vj) || vi.intersects(*vj) {
                            continue;
                        }
                        let vset = *vi | *vj;
                        for (eid, new_edge) in &edges {
                            // Provided sets must include entire eligibility set.
                            // But either side should not include eligiblity set, that means it is already joined.
                            if vset.includes(new_edge.e_vset)
                                && !vi.includes(new_edge.e_vset)
                                && !vj.includes(new_edge.e_vset)
                            {
                                // then identify left and right
                                let (l_vset, r_vset) = {
                                    let l = new_edge.e_vset & new_edge.l_vset;
                                    let r = new_edge.e_vset & new_edge.r_vset;
                                    if vi.includes(l) && vj.includes(r) {
                                        (*vi, *vj)
                                    } else if vi.includes(r) && vj.includes(l) {
                                        (*vj, *vi)
                                    } else {
                                        return Err(Error::InvalidJoinTransformation);
                                    }
                                };
                                let mut edge = Cow::Borrowed(*new_edge);
                                // Addtional check must be performed on inner join, to ensure all
                                // available edges are merged together before the estimation.
                                if edge.kind == JoinKind::Inner {
                                    for (merge_eid, merge_edge) in &edges {
                                        if merge_edge.kind == JoinKind::Inner
                                            && merge_eid != eid
                                            && vset.includes(merge_edge.e_vset)
                                            && !vi.includes(merge_edge.e_vset)
                                            && !vj.includes(merge_edge.e_vset)
                                        {
                                            let edge = edge.to_mut();
                                            edge.cond.extend_from_slice(&merge_edge.cond);
                                            edge.e_vset |= merge_edge.e_vset;
                                        }
                                    }
                                }
                                // now estimate rows
                                let rows =
                                    self.0.estimate_join_rows(graph, l_vset, r_vset, &edge)?;
                                // check if it's better than existing one and update
                                match target_plan.entry(*vi | *vj) {
                                    Entry::Vacant(vac) => {
                                        let join_edge = JoinEdge {
                                            l_vset,
                                            r_vset,
                                            edge,
                                            rows,
                                            cost: ti.cost() + tj.cost() + rows,
                                        };
                                        vac.insert(Tree::Join(join_edge));
                                    }
                                    Entry::Occupied(mut occ) => {
                                        let new_cost = ti.cost() + tj.cost() + rows;
                                        if occ.get().cost() > new_cost {
                                            let join_edge = JoinEdge {
                                                l_vset,
                                                r_vset,
                                                edge,
                                                rows,
                                                cost: new_cost,
                                            };
                                            *occ.get_mut() = Tree::Join(join_edge);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                // swap target plan back
                mem::swap(&mut target_plan, &mut best_plans[join_size]);
            }
        }
        if best_plans[n_vertexes].len() != 1 {
            return Err(Error::InvalidJoinTransformation);
        }
        let (vset, tree) = best_plans[n_vertexes].iter().next().unwrap();
        build_join_tree(graph, &best_plans, *vset, tree)
    }
}

fn build_join_tree(
    graph: &Graph,
    best_plans: &[HashMap<VertexSet, Tree>],
    vset: VertexSet,
    tree: &Tree,
) -> Result<Op> {
    match tree {
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
            let l_tree = &best_plans[l_vset.len()][l_vset];
            let left = build_join_tree(graph, best_plans, *l_vset, l_tree)?;
            let r_tree = &best_plans[r_vset.len()][r_vset];
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
