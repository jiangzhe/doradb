use crate::error::{Error, Result};
use crate::index::column_block_index::{ColumnBlockIndex, ColumnLeafEntry};
use std::collections::BTreeSet;
use std::mem;

/// Encodes sorted unique deletion deltas into the current checkpoint blob format.
pub fn encode_deletion_deltas_to_bytes(deltas: &BTreeSet<u32>) -> Vec<u8> {
    let mut out = Vec::with_capacity(deltas.len() * mem::size_of::<u32>());
    for delta in deltas {
        out.extend_from_slice(&delta.to_le_bytes());
    }
    out
}

/// Decodes the current checkpoint blob format into sorted unique deletion deltas.
pub fn decode_deletion_deltas_from_bytes(bytes: &[u8]) -> Result<Vec<u32>> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(mem::size_of::<u32>()) {
        return Err(Error::InvalidFormat);
    }
    let mut res = Vec::with_capacity(bytes.len() / mem::size_of::<u32>());
    for chunk in bytes.chunks_exact(mem::size_of::<u32>()) {
        res.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    res.sort_unstable();
    res.dedup();
    Ok(res)
}

/// Loads persisted deletion deltas from either inline payload bytes or an offloaded blob.
pub async fn load_payload_deletion_deltas(
    column_index: &ColumnBlockIndex<'_>,
    entry: ColumnLeafEntry,
) -> Result<BTreeSet<u32>> {
    let deltas = column_index.load_delete_deltas(&entry).await?;
    Ok(deltas.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_deletion_deltas_roundtrip_dedups() {
        let deltas = BTreeSet::from([9, 2, 9, 5]);
        let bytes = encode_deletion_deltas_to_bytes(&deltas);
        assert_eq!(
            decode_deletion_deltas_from_bytes(&bytes).unwrap(),
            vec![2, 5, 9]
        );
    }

    #[test]
    fn test_decode_deletion_deltas_rejects_invalid_bytes() {
        assert!(matches!(
            decode_deletion_deltas_from_bytes(&[]),
            Err(Error::InvalidFormat)
        ));
        assert!(matches!(
            decode_deletion_deltas_from_bytes(&[1, 2, 3]),
            Err(Error::InvalidFormat)
        ));
    }
}
