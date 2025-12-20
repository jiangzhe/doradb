use crate::chan::{InputChannel, OutputChannel};
use crate::exec::{ExecCtx, Executable, Work};
use async_trait::async_trait;
use futures_lite::StreamExt;
use std::sync::Arc;
use doradb_compute::eval::QueryEvalPlan;
use doradb_protocol::mysql::error::{Error, Result};
use doradb_storage::block::Block;

pub struct ProjExec {
    eval_plan: Arc<QueryEvalPlan>,
    input: InputChannel,
    out: Option<OutputChannel>,
    parallel: usize,
    // Whether the projection should reserve order of input data.
    reserve_order: bool,
}

impl ProjExec {
    #[inline]
    pub fn new(
        eval_plan: Arc<QueryEvalPlan>,
        input: InputChannel,
        out: OutputChannel,
        parallel: usize,
        reserve_order: bool,
    ) -> Self {
        ProjExec {
            eval_plan,
            input,
            out: Some(out),
            parallel,
            reserve_order,
        }
    }

    #[inline]
    fn new_work(&self, block: Block) -> ProjWork {
        ProjWork {
            eval_plan: self.eval_plan.clone(),
            block,
        }
    }
}

#[async_trait]
impl Executable for ProjExec {
    #[inline]
    async fn exec(&mut self, ctx: &ExecCtx) {
        let out = if let Some(out) = self.out.take() {
            out
        } else {
            log::error!("Projection executor starts more than once");
            ctx.cancel.cancel(Error::InvalidExecutorState());
            return;
        };
        let mut in_stream = self.input.to_stream(&ctx.cancel);
        if self.reserve_order {
            let mut dispatcher = ctx.dispatch_ordered(self.parallel, out);
            while let Some(Ok(input)) = in_stream.next().await {
                let work = self.new_work(input);
                dispatcher.dispatch(work).await;
            }
        } else {
            let mut dispatcher = ctx.dispatch_unordered(self.parallel, out);
            while let Some(Ok(input)) = in_stream.next().await {
                let work = self.new_work(input);
                dispatcher.dispatch(work).await;
            }
        }
    }
}

struct ProjWork {
    eval_plan: Arc<QueryEvalPlan>,
    block: Block,
}

#[async_trait]
impl Work for ProjWork {
    #[inline]
    async fn run(self) -> Result<Block> {
        let n_records = self.block.n_records;
        self.eval_plan
            .eval(&self.block)
            .map(|data| Block::new(n_records, data))
            .map_err(|e| Error::RuntimeError(Box::new(e.to_string())))
    }
}
