//! Streaming content verification shared by operation benchmarks.

use crate::error::{BenchError, Result};
use doradb_storage::id::TableID;
use doradb_storage::{CallbackResult, IndexID, ScanRowDecision, Session, TableIndex, Val};
use std::fmt::Write;
use xxhash_rust::xxh3::Xxh3Default;

/// Order-independent sum of length-delimited row hashes and exact count.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Fingerprint {
    rows: u64,
    sum: u128,
}

impl Fingerprint {
    /// Return the checked number of hashed rows.
    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    fn add_row(&mut self, row: &[Val]) -> Result<()> {
        let [key, payload] = row else {
            return Err(BenchError::message(
                "content verification requires exactly two columns",
            ));
        };
        let key = key.as_u64().ok_or_else(|| {
            BenchError::message("content verification requires an unsigned 64-bit key")
        })?;
        let payload = payload
            .as_bytes()
            .ok_or_else(|| BenchError::message("content verification requires a byte payload"))?;
        let length = u64::try_from(payload.len())
            .map_err(|_| BenchError::message("verification payload length overflow"))?;
        let rows = self
            .rows
            .checked_add(1)
            .ok_or_else(|| BenchError::message("content verification row count overflow"))?;
        let mut hasher = Xxh3Default::new();
        hasher.update(&key.to_le_bytes());
        hasher.update(&length.to_le_bytes());
        hasher.update(payload);
        self.sum = self.sum.wrapping_add(hasher.digest128());
        self.rows = rows;
        Ok(())
    }

    /// Return the multiplicity-preserving digest as hexadecimal.
    pub(crate) fn hex(&self) -> String {
        let mut hex = String::with_capacity(32);
        for byte in self.sum.to_le_bytes() {
            // Writing into a String is infallible.
            let _ = write!(hex, "{byte:02x}");
        }
        hex
    }
}

/// Drain a table or stable-index scan and settle its transaction on every path.
pub(crate) async fn scan_content(
    session: &mut Session,
    table_id: TableID,
    index_id: Option<IndexID>,
) -> Result<Fingerprint> {
    let mut trx = session.begin_trx()?;
    let result = async {
        let mut content = Fingerprint::default();
        if let Some(index_id) = index_id {
            let mut stream = trx
                .table_index_scan_mvcc_stream(TableIndex(table_id, index_id), .., &[0, 1])
                .await?;
            while let Some(row) = stream.next().await? {
                content.add_row(&row)?;
            }
        } else {
            let mut stream = trx
                .table_scan_mvcc_stream(table_id, &[0, 1], |_| -> CallbackResult<_> {
                    Ok(ScanRowDecision::Include)
                })
                .await?;
            while let Some(row) = stream.next().await? {
                content.add_row(&row)?;
            }
        }
        Ok::<_, BenchError>(content)
    }
    .await;
    match result {
        Ok(content) => {
            trx.commit().await?;
            Ok(content)
        }
        Err(error) => {
            let _ = trx.rollback().await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fingerprint(rows: &[(u64, &[u8])]) -> Fingerprint {
        let mut result = Fingerprint::default();
        for (key, payload) in rows {
            result
                .add_row(&[Val::from(*key), Val::from(*payload)])
                .unwrap();
        }
        result
    }

    /// Purpose: Fingerprint row multisets independently of traversal order.
    /// Expected: Canonical encoding preserves content and multiplicity distinctions while
    /// ignoring row order.
    #[test]
    fn fingerprints_preserve_content_and_multiplicity_without_order() {
        let first = (0, &b"hello"[..]);
        let second = (u64::MAX, &b"\0\xff"[..]);
        assert_eq!(fingerprint(&[first, second]), fingerprint(&[second, first]));
        assert_ne!(fingerprint(&[first, first]), fingerprint(&[first]));
        assert_ne!(fingerprint(&[first]), fingerprint(&[(1, first.1)]));
        assert_ne!(fingerprint(&[first]), fingerprint(&[(0, b"hell")]));
        assert_ne!(fingerprint(&[(0, b"")]), fingerprint(&[]));
        assert_ne!(
            fingerprint(&[(0, b"a"), (1, b"bc")]),
            fingerprint(&[(0, b"ab"), (1, b"c")])
        );
        assert_eq!(fingerprint(&[]).hex(), "0".repeat(32));
        // Independent upstream C XXH3_128bits vector for key=0, length=5, payload="hello".
        assert_eq!(
            fingerprint(&[first]).hex(),
            "6086575bdd745f4ed984086493b96fd8"
        );
    }

    /// Purpose: Protect fingerprint validity while distinguishing checked counts from modular
    /// digest arithmetic.
    /// Expected: Malformed rows fail, count overflow preserves state, and digest accumulation
    /// wraps as specified.
    #[test]
    fn fingerprint_rejects_bad_values_and_checked_count_overflow() {
        for row in [
            vec![],
            vec![Val::from(0u64)],
            vec![Val::from(0u32), Val::from("x")],
            vec![Val::from(0u64), Val::from(1u64)],
        ] {
            assert!(Fingerprint::default().add_row(&row).is_err());
        }
        let mut full = Fingerprint {
            rows: u64::MAX,
            sum: u128::MAX,
        };
        let before = full.clone();
        assert!(full.add_row(&[Val::from(0u64), Val::from("")]).is_err());
        assert_eq!(full, before);
        // Digest addition intentionally wraps; only the independent row count is checked.
        full.rows = 0;
        full.add_row(&[Val::from(0u64), Val::from("")]).unwrap();
        let expected = fingerprint(&[(0, b"")]);
        let minus_one = expected.sum.wrapping_sub(1);
        assert_eq!(full.sum, minus_one);
    }
}
