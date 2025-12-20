//! Execution implementation of physical plans.
//!
//! Each Physical node will be converted into an execution node.
//! All nodes compose a DAG and data flows from upstream such as
//! table scans to downstream.
mod builder;
mod proj;

use crate::cancel::Cancellation;
use crate::chan::OutputChannel;
use crate::exec::builder::ExecBuilder;
use async_executor::Executor;
use async_trait::async_trait;
use flume::{Receiver, Sender};
use futures_lite::Stream;
pub use proj::ProjExec;
use std::collections::VecDeque;
use std::sync::Arc;
// use doradb_plan::phy::PhyPlan;
use doradb_protocol::mysql::error::Result;
use doradb_storage::block::Block;

pub struct ExecPlan {
    /// topology sorted executable nodes.
    nodes: VecDeque<Exec>,
}

impl ExecPlan {
    #[inline]
    pub fn new(phy: PhyPlan) -> Result<Self> {
        ExecBuilder::new(&phy).build()
    }
}

pub enum Exec {
    Proj(ProjExec),
}

pub struct ExecCtx<'a> {
    pub(crate) executor: Arc<Executor<'a>>,
    pub(crate) cancel: Cancellation,
}

impl<'a> ExecCtx<'a> {
    /// Create workers by given parallelism, and returns task dispatcher.
    /// each worker will complete its work and send result to output channel.
    /// This method will not reserve input order.
    #[inline]
    pub(crate) fn dispatch_unordered<W: Work>(
        &self,
        parallel: usize,
        out: OutputChannel,
    ) -> UnorderedDispatcher<W> {
        let (work_tx, work_rx) = flume::bounded::<W>(parallel);
        for _ in 0..parallel {
            let work_rx = work_rx.clone();
            let out = out.clone();
            let cancel = self.cancel.clone();
            self.executor
                .spawn(async move {
                    while let Ok(work) = work_rx.recv_async().await {
                        match work.run().await {
                            Ok(res) => {
                                if out.send(res).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                cancel.cancel(e);
                                return;
                            }
                        }
                    }
                })
                .detach();
        }
        UnorderedDispatcher { tx: work_tx }
    }

    /// Create workers by given parallelism, and returns task dispatcher.
    /// Each work will returned by a onetime channel and transfered one by one.
    /// So the output will reserve input order.
    #[inline]
    pub(crate) fn dispatch_ordered<W: Work>(
        &self,
        parallel: usize,
        out: OutputChannel,
    ) -> OrderedDispatcher<W> {
        let (in_tx, in_rx) = flume::bounded::<(W, Sender<Result<Block>>)>(parallel);
        let (out_tx, out_rx) = flume::bounded::<Receiver<Result<Block>>>(parallel);
        // setup workers
        for _ in 0..parallel {
            let in_rx = in_rx.clone();
            self.executor
                .spawn(async move {
                    while let Ok((work, res_tx)) = in_rx.recv_async().await {
                        let res = work.run().await;
                        if res_tx.send_async(res).await.is_err() {
                            return;
                        }
                    }
                })
                .detach();
        }
        // single task to collect result and send to output channel
        // it's used to reserve input order.
        let cancel = self.cancel.clone();
        self.executor
            .spawn(async move {
                while let Ok(res) = out_rx.recv_async().await {
                    // each work only has one output
                    if let Ok(res) = res.recv_async().await {
                        match res {
                            Ok(block) => {
                                if out.send(block).await.is_err() {
                                    return;
                                }
                            }
                            Err(e) => {
                                cancel.cancel(e);
                                return;
                            }
                        }
                    }
                }
            })
            .detach();

        OrderedDispatcher { in_tx, out_tx }
    }
}

#[async_trait]
pub trait Executable {
    async fn exec(&mut self, ctx: &ExecCtx);
}

pub struct UnorderedDispatcher<W> {
    tx: Sender<W>,
}

impl<W: Work> UnorderedDispatcher<W> {
    /// Dispatch the work to underlying executor in background.
    #[inline]
    pub async fn dispatch(&mut self, work: W) {
        // ignore dispatch error is ok
        let _ = self.tx.send_async(work).await;
    }
}

pub struct OrderedDispatcher<W> {
    in_tx: Sender<(W, Sender<Result<Block>>)>,
    out_tx: Sender<Receiver<Result<Block>>>,
}

impl<W: Work> OrderedDispatcher<W> {
    #[inline]
    pub async fn dispatch(&mut self, work: W) {
        // make output in same order as input
        let (tx, rx) = flume::bounded(1);
        if self.in_tx.send_async((work, tx)).await.is_ok() {
            // ignore dispatch error is ok
            let _ = self.out_tx.send_async(rx).await;
        }
    }
}

/// Single unit of computation.
#[async_trait]
pub trait Work: Sized + Send + Sync + 'static {
    /// run the work to get output
    async fn run(self) -> Result<Block>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chan;
    use crate::tests::single_thread_executor;
    use futures_lite::future;
    use futures_lite::StreamExt;
    use doradb_compute::eval::QueryEvalPlan;
    use doradb_datatype::PreciseType;
    use doradb_expr::{
        Col, ColIndex, ColKind, Const, ExprKind, FuncKind, GlobalID, QueryID, TypeFix, TypeInferer,
    };
    use doradb_storage::attr::Attr;

    #[test]
    fn test_proj_exec() {
        let size = 1024i32;
        let attr1 = Attr::from((0..size).into_iter().map(|i| i as i64));
        let attr2 = attr1.to_owned();
        let block = Block::new(1024, vec![attr1, attr2]);
        let col1 = ExprKind::query_col(GlobalID::from(1), QueryID::from(0), ColIndex::from(0));
        // select c1 + 1
        let es = vec![ExprKind::func(
            FuncKind::Add,
            vec![col1.clone(), ExprKind::Const(Const::I64(1))],
        )];
        let eval_plan = build_plan(es);
        let (upstream_in, upstream_out) = chan::unbounded(1024);
        let (downstream_in, downstream_out) = chan::unbounded(1024);
        let mut proj_exec =
            ProjExec::new(Arc::new(eval_plan), upstream_in, downstream_out, 1, false);
        // setup execution context
        let executor = single_thread_executor();
        let cancel = Cancellation::new();
        let ctx = ExecCtx { executor, cancel };
        future::block_on(async move {
            upstream_out.send(block).await.unwrap();
            // end upstream
            drop(upstream_out);
            proj_exec.exec(&ctx).await;
            let mut stream = downstream_in.to_stream(&ctx.cancel);
            let res = stream.next().await.unwrap();
            if let Ok(res) = res {
                assert_eq!(1024, res.n_records);
                let res = res.data;
                assert_eq!(1, res.len());
                assert_eq!(PreciseType::i64(), res[0].ty);
                let data: &[i64] = res[0].codec.as_array().unwrap().cast_slice();
                assert!(res[0].validity.is_all());
                let expected: Vec<_> = (0..1024).into_iter().map(|i| (i + 1) as i64).collect();
                assert_eq!(&expected, data);
            } else {
                panic!("failed")
            }
        })
    }

    #[inline]
    fn build_plan(mut exprs: Vec<ExprKind>) -> QueryEvalPlan {
        let mut inferer = IntColInferer;
        for e in &mut exprs {
            e.fix(&mut inferer).unwrap();
        }
        QueryEvalPlan::new(&exprs, &mut inferer).unwrap()
    }

    struct IntColInferer;
    impl TypeInferer for IntColInferer {
        fn confirm(&mut self, e: &ExprKind) -> Option<PreciseType> {
            match e {
                ExprKind::Col(Col { kind, .. }) => match kind {
                    ColKind::Query(..) | ColKind::Correlated(..) => Some(PreciseType::i64()),
                    _ => None,
                },
                _ => None,
            }
        }
    }
}
