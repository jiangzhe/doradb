use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::iter::FromIterator;
use doradb_storage::bitmap::Bitmap;

fn bench_extend(c: &mut Criterion) {
    for log2_size in [12, 14] {
        let size = 2usize.pow(log2_size);
        let bm2 = Bitmap::from_iter((0..size).into_iter().map(|x| x & 3 == 0));
        c.bench_function(&format!("bitmap_extend_aligned_{}", size), |b| {
            let mut bm1 = Bitmap::with_capacity(size);
            b.iter(black_box(|| {
                bm1.extend(&bm2);
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_unaligned_{}", size), |b| {
            let mut bm1 = Bitmap::with_capacity(size);
            b.iter(black_box(|| {
                bm1.add(true);
                bm1.extend(&bm2);
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_const_aligned_{}", size), |b| {
            let mut bm1 = Bitmap::with_capacity(size);
            b.iter(black_box(|| {
                bm1.extend_const(true, size);
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_const_unaligned_{}", size), |b| {
            let mut bm1 = Bitmap::with_capacity(size);
            b.iter(black_box(|| {
                bm1.add(true);
                bm1.extend_const(true, size);
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_range_aligned_{}", size), |b| {
            let mut bm1 = Bitmap::with_capacity(size);
            b.iter(black_box(|| {
                bm1.extend_range(&bm2, 256..size).unwrap();
                bm1.clear();
            }))
        });

        c.bench_function(&format!("bitmap_extend_range_unaligned_{}", size), |b| {
            let mut bm1 = Bitmap::with_capacity(size);
            b.iter(black_box(|| {
                bm1.extend_range(&bm2, 255..size).unwrap();
                bm1.clear();
            }))
        });
    }
}

criterion_group!(bench_extend_group, bench_extend);
criterion_main!(bench_extend_group);
