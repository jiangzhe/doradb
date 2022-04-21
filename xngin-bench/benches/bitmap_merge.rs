use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::iter::FromIterator;
use xngin_common::bitmap::Bitmap;

fn bench_merge(c: &mut Criterion) {
    for log2_size in [12, 14] {
        let size = 2usize.pow(log2_size);
        let mut bm1 = Bitmap::from_iter((0..size).into_iter().map(|x| x & 3 == 1));
        let bm2 = Bitmap::from_iter((0..size).into_iter().map(|x| x & 3 == 0));
        c.bench_function(&format!("bitmap_merge_{}", size), |b| {
            b.iter(black_box(|| {
                bm1.merge(&bm2).unwrap();
            }))
        });
    }
}

criterion_group!(bench_merge_group, bench_merge);
criterion_main!(bench_merge_group);
