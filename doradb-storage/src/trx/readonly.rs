use crate::id::TrxID;
use crate::quiescent::QuiescentGuard;
use crate::trx::sys::TransactionSystem;

/// Maintenance-only snapshot registered in the active GC horizon.
///
/// This owner carries no transaction capabilities or stable session child
/// state. Its active STS protects root snapshots borrowed from it until drop.
pub(crate) struct PrivateSnapshot {
    trx_sys: QuiescentGuard<TransactionSystem>,
    sts: TrxID,
    gc_no: usize,
}

impl PrivateSnapshot {
    /// Return the registered snapshot timestamp.
    #[inline]
    pub(crate) fn sts(&self) -> TrxID {
        self.sts
    }
}

impl Drop for PrivateSnapshot {
    #[inline]
    fn drop(&mut self) {
        self.trx_sys.deregister_active_sts(self.gc_no, self.sts);
    }
}

impl QuiescentGuard<TransactionSystem> {
    /// Register one mandatory-maintenance snapshot in the active GC horizon.
    #[inline]
    pub(crate) fn register_private_snapshot(&self) -> PrivateSnapshot {
        let (gc_no, sts) = self.register_active_sts();
        PrivateSnapshot {
            trx_sys: self.clone(),
            sts,
            gc_no,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::trx::{
        MAX_SNAPSHOT_TS,
        tests::{active_sts_count, test_engine},
    };

    #[test]
    fn test_private_snapshot_registers_and_releases_active_sts() {
        smol::block_on(async {
            let (_temp_dir, engine) = test_engine("private_snapshot_sts").await;
            let trx_sys = engine.inner().trx_sys.clone();
            assert_eq!(active_sts_count(&trx_sys), 0);

            let first = trx_sys.register_private_snapshot();
            let first_sts = first.sts();
            assert_eq!(active_sts_count(&trx_sys), 1);
            assert_eq!(trx_sys.min_active_sts(), first_sts);

            let second = trx_sys.register_private_snapshot();
            let second_sts = second.sts();
            assert!(second_sts > first_sts);
            assert_eq!(active_sts_count(&trx_sys), 2);
            assert_eq!(trx_sys.min_active_sts(), first_sts);

            drop(first);
            assert_eq!(active_sts_count(&trx_sys), 1);
            assert_eq!(trx_sys.min_active_sts(), second_sts);
            drop(second);
            assert_eq!(active_sts_count(&trx_sys), 0);
            assert_eq!(trx_sys.min_active_sts(), MAX_SNAPSHOT_TS);

            drop(trx_sys);
            engine.shutdown();
        });
    }
}
