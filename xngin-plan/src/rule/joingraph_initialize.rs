use crate::error::{Error, Result};
use crate::join::graph::{qids_to_vset, Edge, VertexID, VertexSet, MAX_JOIN_QUERIES};
use crate::join::{Join, JoinGraph, JoinKind, JoinOp, QualifiedJoin};
use crate::op::{Op, OpMutVisitor};
use crate::query::{Location, QuerySet};
use bitflags::bitflags;
use indexmap::IndexMap;
use std::collections::{HashMap, HashSet};
use std::mem;
use xngin_expr::controlflow::{Branch, ControlFlow, Unbranch};
use xngin_expr::{Expr, QueryID};

bitflags! {
    struct Spec: u8 {
        /// A means Add.
        /// The operator will add rows of NULLs and output
        const A = 0x01;
        /// D means Delete.
        /// The operator will remove rows that are matched or unmatched.
        const D = 0x02;
        /// X means Exclude.
        /// The operator will exclude entire data set, this is used
        /// for SemiJoin, AntiJoin.
        const X = 0x04;
    }
}

trait JoinSpec {
    fn left_spec(&self) -> Spec;
    fn right_spec(&self) -> Spec;
}

impl JoinSpec for JoinKind {
    #[inline]
    fn left_spec(&self) -> Spec {
        match self {
            JoinKind::Inner => Spec::D,
            JoinKind::Left => Spec::empty(),
            JoinKind::Full => Spec::A,
            _ => todo!(),
        }
    }

    #[inline]
    fn right_spec(&self) -> Spec {
        match self {
            JoinKind::Inner => Spec::D,
            JoinKind::Left => Spec::A | Spec::D,
            JoinKind::Full => Spec::A,
            _ => todo!(),
        }
    }
}

/// Initialize join graph.
/// The process can be viewed as two actions:
/// 1. detect joins.
/// Traverse the operator tree down to find the first join operator.
/// Then start the initialization.
/// 2. initialize graph.
/// From the topmost join operator, collect all queries to be joined.
/// Then replace the topmost join with the generated graph.
#[inline]
pub fn joingraph_initialize(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    init_joingraph(qry_set, qry_id)
}

fn init_joingraph(qry_set: &mut QuerySet, qry_id: QueryID) -> Result<()> {
    qry_set.transform_op(qry_id, |qry_set, location, op| {
        if location == Location::Intermediate {
            // only build join graph in intermediate queries
            let mut init = InitGraph { qry_set };
            op.walk_mut(&mut init).unbranch()
        } else {
            Ok(())
        }
    })?
}

struct InitGraph<'a> {
    qry_set: &'a mut QuerySet,
}

impl OpMutVisitor for InitGraph<'_> {
    type Cont = ();
    type Break = Error;
    #[inline]
    fn enter(&mut self, op: &mut Op) -> ControlFlow<Error> {
        match op {
            Op::Join(join) => {
                let builder = BuildGraph::new(self.qry_set);
                let graph = builder.build(join.as_mut()).branch()?;
                *op = Op::join_graph(graph);
                ControlFlow::Continue(())
            }
            Op::Query(qry_id) => {
                // no join in current tree, recursively detect all children
                init_joingraph(self.qry_set, *qry_id).branch()
            }
            _ => ControlFlow::Continue(()),
        }
    }
}

struct BuildGraph<'a> {
    qry_set: &'a mut QuerySet,
    vertexes: VertexSet,
    vmap: HashMap<VertexID, QueryID>,
    rev_vmap: HashMap<QueryID, VertexID>,
    edges: IndexMap<VertexSet, Vec<Edge>>,
    queries: Vec<Op>,
}

impl<'a> BuildGraph<'a> {
    fn new(qry_set: &'a mut QuerySet) -> Self {
        BuildGraph {
            qry_set,
            vertexes: VertexSet::default(),
            vmap: HashMap::new(),
            rev_vmap: HashMap::new(),
            edges: IndexMap::new(),
            queries: vec![],
        }
    }
}

impl BuildGraph<'_> {
    fn build(mut self, join: &mut Join) -> Result<JoinGraph> {
        let _ = self.process_join(join)?;
        Ok(JoinGraph {
            vertexes: self.vertexes,
            vmap: self.vmap,
            rev_vmap: self.rev_vmap,
            edges: self.edges,
            queries: self.queries,
        })
    }

    // Process join, add vertexes and edges into join graph.
    // Returns nodes of entire tree covered by the join.
    fn process_join(&mut self, join: &mut Join) -> Result<VertexSet> {
        match join {
            Join::Cross(_) => Err(Error::CrossJoinNotSupport),
            Join::Qualified(QualifiedJoin {
                kind,
                left,
                right,
                cond,
                filt,
            }) => {
                let l_vset = self.process_jo(left)?;
                let r_vset = self.process_jo(right)?;
                match *kind {
                    JoinKind::Inner => {
                        // Here we need to analyze join conditions to identify hidden edges.
                        // e.g. "A JOIN B ON a1 = b1 JOIN C ON b1 = c1"
                        // With predicates pullup, we will propagate a new join predicate
                        // "a1 = c1" and added to the second join.
                        // This is a new edge we can use to directly join A and C.
                        // Join edge must has at least one equation between columns on both sides.
                        let mut vset_conds: IndexMap<VertexSet, Vec<Expr>> = IndexMap::new();
                        let mut tmp_qset = HashSet::new();
                        // inner join has no filter predicates.
                        assert!(filt.is_empty());
                        for c in mem::take(cond) {
                            tmp_qset.clear();
                            // let eq = analyze_cond(&c, &mut tmp_qset);
                            c.collect_qry_ids(&mut tmp_qset);
                            // for inner join, there must be two tables involved in join condition,
                            // otherwise, the predicate can be pushed down
                            assert!(tmp_qset.len() > 1);
                            let e_vset = qids_to_vset(&self.rev_vmap, &tmp_qset)?;
                            vset_conds.entry(e_vset).or_default().push(c);
                        }
                        for (e_vset, cond) in vset_conds {
                            self.add_edge(*kind, l_vset, r_vset, e_vset, cond, vec![]);
                        }
                    }
                    JoinKind::Left | JoinKind::Full => {
                        // we need to process join conditions ad filters together.
                        let mut tmp_qset = HashSet::new();
                        let cond = mem::take(cond);
                        let filt = mem::take(filt);
                        for p in cond.iter().chain(filt.iter()) {
                            p.collect_qry_ids(&mut tmp_qset);
                        }
                        let e_vset = qids_to_vset(&self.rev_vmap, &tmp_qset)?;
                        self.add_edge(*kind, l_vset, r_vset, e_vset, cond, filt);
                    }
                    _ => todo!(),
                }
                let vset = l_vset | r_vset;
                Ok(vset)
            }
        }
    }

    fn add_edge(
        &mut self,
        kind: JoinKind,
        l_vset: VertexSet,
        r_vset: VertexSet,
        mut e_vset: VertexSet,
        cond: Vec<Expr>,
        filt: Vec<Expr>,
    ) {
        e_vset |= self.update_elig_set(kind.left_spec(), l_vset, e_vset & l_vset);
        e_vset |= self.update_elig_set(kind.right_spec(), r_vset, e_vset & r_vset);
        let edge = Edge {
            kind,
            l_vset,
            r_vset,
            e_vset,
            cond,
            filt,
        };
        let edges = self.edges.entry(l_vset | r_vset).or_default();
        // only inner join could be separated to multiple edges upon same join tree.
        assert!(edges.iter().all(|e| e.kind == JoinKind::Inner));
        edges.push(edge);
    }

    fn update_elig_set(&self, spec: Spec, vset: VertexSet, mut join_vset: VertexSet) -> VertexSet {
        if spec.contains(Spec::X) {
            join_vset |= vset;
            return join_vset;
        }
        if vset.len() == 1 {
            return join_vset;
        }
        let edges = self.edges.get(&vset).unwrap();
        for edge in edges {
            if spec.contains(Spec::A) {
                self.extend_elig(Spec::A, Spec::D, edge, &mut join_vset);
            }
            if spec.contains(Spec::D) {
                self.extend_elig(Spec::D, Spec::A, edge, &mut join_vset);
            }
        }
        join_vset
    }

    #[inline]
    fn extend_elig(&self, base: Spec, rev: Spec, edge: &Edge, join_vset: &mut VertexSet) {
        match (
            join_vset.intersects(edge.l_vset),
            join_vset.intersects(edge.r_vset),
        ) {
            (true, true) => {
                self.extend_left_elig(base, rev, edge, join_vset);
                self.extend_right_elig(base, rev, edge, join_vset);
            }
            (true, false) => {
                self.extend_left_elig(base, rev, edge, join_vset);
                // check right again
                if join_vset.intersects(edge.r_vset) {
                    self.extend_right_elig(base, rev, edge, join_vset);
                }
            }
            (false, true) => {
                self.extend_right_elig(base, rev, edge, join_vset);
                // check left again
                if join_vset.intersects(edge.l_vset) {
                    self.extend_left_elig(base, rev, edge, join_vset);
                }
            }
            (false, false) => unreachable!(),
        }
    }

    #[inline]
    fn extend_left_elig(&self, base: Spec, rev: Spec, edge: &Edge, join_vset: &mut VertexSet) {
        if edge.kind.left_spec().contains(rev) {
            *join_vset |= edge.e_vset;
        } else {
            *join_vset |= self.update_elig_set(base, edge.l_vset, *join_vset & edge.l_vset);
        }
    }

    #[inline]
    fn extend_right_elig(&self, base: Spec, rev: Spec, edge: &Edge, join_vset: &mut VertexSet) {
        if edge.kind.right_spec().contains(rev) {
            *join_vset |= edge.e_vset;
        } else if edge.r_vset.len() > 1 {
            *join_vset |= self.update_elig_set(base, edge.r_vset, *join_vset & edge.r_vset);
        }
    }

    fn process_jo(&mut self, jo: &mut JoinOp) -> Result<VertexSet> {
        let mut vset = VertexSet::default();
        match jo {
            JoinOp(Op::Query(qry_id)) => {
                // recursively build join group in derived table
                init_joingraph(self.qry_set, *qry_id)?;
                let vid = self.add_query(*qry_id)?;
                vset |= vid;
            }
            JoinOp(Op::Join(join)) => {
                // recursively add children to join graph
                let child_vset = self.process_join(join.as_mut())?;
                vset |= child_vset;
            }
            _ => unreachable!(),
        }
        Ok(vset)
    }

    fn add_query(&mut self, qry_id: QueryID) -> Result<VertexID> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::tests::{assert_j_plan1, get_join_graph, j_catalog, print_plan};
    use crate::query::QueryPlan;
    use crate::rule::derived_unfold;

    #[test]
    fn test_joingraph_init_simple() {
        let cat = j_catalog();
        assert_j_plan1(&cat, "select 1 from t1 join t2", assert_join_graph_exists);
        assert_j_plan1(
            &cat,
            "select 1 from t1 left join t2",
            assert_join_graph_exists,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 right join t2",
            assert_join_graph_exists,
        );
        assert_j_plan1(
            &cat,
            "select 1 from t1 full join t2",
            assert_join_graph_exists,
        );
    }

    #[test]
    fn test_joingraph_init_inner_inner() {
        let cat = j_catalog();
        assert_j_plan1(
            &cat,
            "select 1 from t1 join t2 on t1.c1 = t2.c2 join t3 on t2.c2 = t3.c3",
            |s1, mut q1| {
                derived_unfold(&mut q1.qry_set, q1.root).unwrap();
                joingraph_initialize(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                // we have 2 inner edge
                let subq = q1.root_query().unwrap();
                let g = get_join_graph(subq).unwrap();
                assert_eq!(2, g.edges.len());
                assert_eq!(2, count_edges(&g.edges, |e| e.kind == JoinKind::Inner));
                assert_any_edge(&g.edges, |e| {
                    if e.kind == JoinKind::Inner {
                        // that means, 2 joins can be reordered
                        assert_eq!(2, e.e_vset.len());
                    }
                })
            },
        )
    }

    #[test]
    fn test_joingraph_init_left_inner() {
        let cat = j_catalog();
        assert_j_plan1(&cat,
            "select 1 from t1 left join (select t1.c1, t2.c2 from t1 join t2 on t1.c1 = t2.c1) tt on t1.c1 = tt.c1",
            |s1, mut q1| {
                derived_unfold(&mut q1.qry_set, q1.root).unwrap();
                joingraph_initialize(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                // we have 1 inner edge: l=1, r=1, e=2, 1 left edge: l=1, r=2, e=3
                let subq = q1.root_query().unwrap();
                let g = get_join_graph(subq).unwrap();
                assert_eq!(2, g.edges.len());
                assert_eq!(1, count_edges(&g.edges, |e| e.kind == JoinKind::Inner));
                assert_eq!(1, count_edges(&g.edges, |e| e.kind == JoinKind::Left));
                assert_any_edge(&g.edges, |e| {
                    if e.kind == JoinKind::Left {
                        // that means, left join must be topmost, reorder is impossible
                        assert_eq!(3, e.e_vset.len());
                    }
                })
            }
        )
    }

    #[test]
    fn test_joingraph_init_inner_left() {
        let cat = j_catalog();
        assert_j_plan1(&cat,
            "select 1 from t1 join (select t1.c1 from t1 left join t2 on t1.c1 = t2.c1) tt on t1.c1 = tt.c1",
            |s1, mut q1| {
                derived_unfold(&mut q1.qry_set, q1.root).unwrap();
                joingraph_initialize(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                // we have 1 left edge: l=1, r=1, e=2, 1 inner edge: l=1, r=2, e=2
                let subq = q1.root_query().unwrap();
                let g = get_join_graph(subq).unwrap();
                assert_eq!(2, g.edges.len());
                assert_eq!(1, count_edges(&g.edges, |e| e.kind == JoinKind::Inner));
                assert_eq!(1, count_edges(&g.edges, |e| e.kind == JoinKind::Left));
                assert_any_edge(&g.edges, |e| {
                    if e.kind == JoinKind::Inner {
                        // that means, left and inner can be reordered
                        assert_eq!(2, e.e_vset.len());
                    }
                })
            }
        )
    }

    #[test]
    fn test_joingraph_init_inner_full_inner() {
        let cat = j_catalog();
        assert_j_plan1(&cat,
            "select 1 from t0 join t1 on t0.c0 = t1.c0 full join (select t2.c2 from t2 join t3 on t2.c2 = t3.c2) tt on t1.c1 = tt.c2",
            |s1, mut q1| {
                derived_unfold(&mut q1.qry_set, q1.root).unwrap();
                joingraph_initialize(&mut q1.qry_set, q1.root).unwrap();
                print_plan(s1, &q1);
                // we have 2 inner edges, 1 full edge
                let subq = q1.root_query().unwrap();
                let g = get_join_graph(subq).unwrap();
                assert_eq!(3, g.edges.len());
                assert_eq!(2, count_edges(&g.edges, |e| e.kind == JoinKind::Inner));
                assert_eq!(1, count_edges(&g.edges, |e| e.kind == JoinKind::Full));
                assert_any_edge(&g.edges, |e| {
                    if e.kind == JoinKind::Inner {
                        assert_eq!(2, e.e_vset.len());
                    } else if e.kind == JoinKind::Full {
                        // that means full join must be topmost, so no reorder can be performed
                        assert_eq!(4, e.e_vset.len());
                    }
                })
            }
        )
    }

    fn assert_join_graph_exists(sql: &str, mut plan: QueryPlan) {
        joingraph_initialize(&mut plan.qry_set, plan.root).unwrap();
        print_plan(sql, &plan);
        let subq = plan.root_query().unwrap();
        assert!(get_join_graph(&subq).is_some());
    }

    fn count_edges<F>(edges: &IndexMap<VertexSet, Vec<Edge>>, f: F) -> usize
    where
        F: Fn(&Edge) -> bool,
    {
        edges
            .values()
            .map(|es| es.iter().filter(|e| f(e)).count())
            .sum()
    }

    fn assert_any_edge<F>(edges: &IndexMap<VertexSet, Vec<Edge>>, f: F)
    where
        F: Fn(&Edge),
    {
        for es in edges.values() {
            for e in es {
                f(e)
            }
        }
    }
}
