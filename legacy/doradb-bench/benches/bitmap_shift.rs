use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::iter::FromIterator;
use doradb_storage::bitmap::Bitmap;

fn bench_shift(c: &mut Criterion) {
    for log2_size in [12, 14] {
        let size = 2usize.pow(log2_size);
        let mut bm1 = Bitmap::from_iter((0..size).into_iter().map(|x| x & 3 == 0));
        c.bench_function(&format!("bitmap_shift_{}_aligned_{}", 8, size), |b| {
            b.iter(black_box(|| {
                bm1.shift(8);
                unsafe { bm1.set_len(size) };
            }))
        });

        c.bench_function(&format!("bitmap_shift_{}_unaligned_{}", 1, size), |b| {
            b.iter(black_box(|| {
                bm1.shift(1);
                unsafe { bm1.set_len(size) };
            }))
        });

        c.bench_function(&format!("bitmap_shift_{}_unaligned_{}", 255, size), |b| {
            b.iter(black_box(|| {
                bm1.shift(255);
                unsafe { bm1.set_len(size) };
            }))
        });
    }
}

criterion_group!(bench_shift_group, bench_shift);
criterion_main!(bench_shift_group);
