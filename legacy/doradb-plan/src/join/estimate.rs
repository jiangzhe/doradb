use crate::error::Result;
use crate::join::graph::{Edge, Graph, VertexSet};

/// Estimates the row count of queries and joins.
/// Query is represented by VertexSet and Join is represented
/// by two VertexSet with join conditions and filter predicates.
pub trait Estimate {
    fn estimate_qry_rows(&mut self, vset: VertexSet) -> Result<f64>;

    fn estimate_join_rows(
        &mut self,
        graph: &Graph,
        l_vset: VertexSet,
        r_vset: VertexSet,
        edge: &Edge,
    ) -> Result<f64>;
}
