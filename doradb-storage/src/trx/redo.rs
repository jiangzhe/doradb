use crate::buffer::page::PageID;
use crate::row::ops::UpdateCol;
use crate::row::RowID;
use crate::trx::TrxID;
use crate::value::Val;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum RedoKind {
    Insert(Vec<Val>),
    Delete,
    Update(Vec<UpdateCol>),
}

#[derive(Serialize, Deserialize)]
pub struct RedoEntry {
    pub page_id: PageID,
    pub row_id: RowID,
    pub kind: RedoKind,
}

#[derive(Serialize, Deserialize)]
pub struct RedoLog {
    pub cts: TrxID,
    pub data: Vec<RedoEntry>,
}

/// RedoBin is serialized redo log in binary format
pub type RedoBin = Vec<u8>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_redo_log_serde_bincode() {
        let mut entries = vec![];
        for _ in 0..102400 {
            let entry = RedoEntry {
                page_id: rand::random::<u32>() as u64,
                row_id: rand::random::<u32>() as u64,
                kind: RedoKind::Insert(vec![Val::Byte8(rand::random::<u32>() as u64)]),
            };
            entries.push(entry);
        }
        const CONFIG: bincode::config::Configuration = bincode::config::standard();
        let start = Instant::now();
        let res = bincode::serde::encode_to_vec(&entries, CONFIG).unwrap();
        let dur = start.elapsed();
        println!(
            "bincode.serialize res.len()={:?}, dur={:?} microseconds, avg {:?} GB/s",
            res.len(),
            dur.as_micros(),
            res.len() as f64 / dur.as_nanos() as f64
        );
        let start = Instant::now();
        let entries: Vec<RedoEntry> = bincode::serde::decode_from_slice(&res, CONFIG).unwrap().0;
        let dur = start.elapsed();
        println!(
            "bincode.deserialize entries.len()={:?}, dur={:?} microseconds, avg {:?} op/s",
            res.len(),
            dur.as_micros(),
            entries.len() as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );
    }

    #[test]
    fn test_redo_log_serde_bitcode() {
        let mut entries = vec![];
        for _ in 0..102400 {
            let entry = RedoEntry {
                page_id: rand::random::<u32>() as u64,
                row_id: rand::random::<u32>() as u64,
                kind: RedoKind::Insert(vec![Val::Byte8(rand::random::<u32>() as u64)]),
            };
            entries.push(entry);
        }
        let start = Instant::now();
        let res = bitcode::serialize(&entries).unwrap();
        let dur = start.elapsed();
        println!(
            "bitcode.serialize res.len()={:?}, dur={:?} microseconds, avg {:?} GB/s",
            res.len(),
            dur.as_micros(),
            res.len() as f64 / dur.as_nanos() as f64
        );
        let start = Instant::now();
        let entries: Vec<RedoEntry> = bitcode::deserialize(&res).unwrap();
        let dur = start.elapsed();
        println!(
            "bitcode.deserialize entries.len()={:?}, dur={:?} microseconds, avg {:?} op/s",
            res.len(),
            dur.as_micros(),
            entries.len() as f64 * 1_000_000_000f64 / dur.as_nanos() as f64
        );
    }
}
