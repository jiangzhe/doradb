use super::*;
use crate::digraph::{DiGraph, NodeIndex};
use crate::error::{Error, Result};
use crate::lgc::{Location, Op, OpKind, QuerySet, Subquery};
use doradb_compute::eval::{QueryEvalPlan, TableEvalPlan};
use doradb_expr::TypeInferer;

pub struct PhyBuilder<'a, I> {
    graph: DiGraph<Phy, ()>,
    start: Vec<NodeIndex>,
    qry_set: &'a QuerySet,
    inferer: &'a I,
}

impl<'a, I: TypeInferer> PhyBuilder<'a, I> {
    #[inline]
    pub fn new(qry_set: &'a QuerySet, inferer: &'a I) -> Self {
        PhyBuilder {
            graph: DiGraph::new(),
            start: vec![],
            qry_set,
            inferer,
        }
    }

    #[inline]
    pub fn build(mut self, q: &Subquery) -> Result<PhyPlan> {
        let end = match q.location {
            Location::Disk => self.build_disk_scan(&q.root)?,
            Location::Intermediate => self.build_intermediate(&q.root)?,
            Location::Virtual => todo!(),
            Location::Memory | Location::Network => todo!(),
        };
        Ok(PhyPlan {
            graph: self.graph,
            start: self.start,
            end,
        })
    }

    // #[inline]
    // fn build_subquery(&mut self, subq: &Subquery) -> Result<NodeIndex> {
    //     match subq.location {
    //         Location::Disk => self.build_disk_scan(&subq.root),
    //         Location::Intermediate => self.build_intermediate(&subq.root),
    //         Location::Virtual => todo!(),
    //         Location::Memory | Location::Network => todo!(),
    //     }
    // }

    // recursively build the intermediate data flow
    #[inline]
    fn build_intermediate(&mut self, root: &Op) -> Result<NodeIndex> {
        match &root.kind {
            // OpKind::Proj { cols, input } => {
            //     let input_idx = self.build_intermediate(input)?;
            //     let evals = QueryEvalPlan::new(cols.iter().map(|c| &c.expr))?;
            //     let proj = Phy {
            //         kind: PhyKind::Proj(PhyProj{evals}),
            //     };
            //     let curr_idx = self.graph.add_node(proj);
            //     self.graph.add_edge(input_idx, curr_idx, ());
            //     Ok(curr_idx)
            // }
            // Op::Query(qry_id) => {
            //     let subq = self
            //         .qry_set
            //         .get(qry_id)
            //         .ok_or(Error::QueryNotFound(*qry_id))?;
            //     self.build_subquery(subq)
            // }
            // Op::Row(cols) => {
            //     // row does not have any input, either query or table is fine.
            //     let evals = QueryEvalPlan::new(cols.iter().map(|c| &c.expr))?;
            //     let row = Phy{
            //         kind: PhyKind::Row(PhyRow{evals}),
            //     };
            //     let curr_idx = self.graph.add_node(row);
            //     Ok(curr_idx)
            // }
            _ => todo!(),
        }
    }

    // Table scan supports Proj and Table operators.
    #[inline]
    fn build_disk_scan(&mut self, mut root: &Op) -> Result<NodeIndex> {
        // let mut proj = None;
        // let table_id;
        // loop {
        //     match &root.kind {
        //         // Op::Proj { cols, input } => {
        //         //     let eval_plan = TableEvalPlan::new(cols.iter().map(|c| &c.expr))?;
        //         //     proj = Some(eval_plan);
        //         //     root = &**input;
        //         // }
        //         OpKind::Table(_, tid) => {
        //             table_id = Some(*tid);
        //             break;
        //         }
        //         _ => return Err(Error::UnsupportedPhyTableScan),
        //     }
        // }
        // let (evals, table_id) = proj.zip(table_id).ok_or(Error::UnsupportedPhyTableScan)?;
        // let node = Phy {
        //     kind: PhyKind::TableScan(PhyTableScan { evals, table_id }),
        // };
        // let idx = self.graph.add_node(node);
        // self.start.push(idx);
        // Ok(idx)
        todo!()
    }
}
