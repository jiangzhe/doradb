use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};
use doradb_storage::bitmap::Bitmap;

fn bench_range_iter(c: &mut Criterion) {
    const N: usize = 10240;
    const PART_N: usize = 256;
    let mut bm0 = Bitmap::with_capacity(N);
    let mut bm1 = Bitmap::with_capacity(N);
    let mut bm2 = Bitmap::with_capacity(N);
    let mut bm3 = Bitmap::with_capacity(N); // continuous bitmap
    let mut thd_rng = thread_rng();
    let mut part = (thd_rng.gen::<bool>(), 0);
    for _ in 0..N {
        bm0.add(thd_rng.gen());
        bm1.add(true);
        bm2.add(false);
        if part.1 == 0 {
            part.0 = !part.0;
            part.1 = thd_rng.gen_range(0..PART_N);
        }
        bm3.add(part.0);
        if part.1 > 0 {
            part.1 -= 1;
        }
    }
    c.bench_function("bitmap_range_iter_rand", |b| {
        b.iter(|| black_box(consume_range_iter(&bm0)))
    });
    c.bench_function("bitmap_range_iter_trues", |b| {
        b.iter(|| black_box(consume_range_iter(&bm1)))
    });
    c.bench_function("bitmap_range_iter_falses", |b| {
        b.iter(|| black_box(consume_range_iter(&bm2)))
    });
    c.bench_function("bitmap_range_iter_continuous", |b| {
        b.iter(|| black_box(consume_range_iter(&bm3)))
    });
}

fn consume_range_iter(bm: &Bitmap) {
    let mut nt = 0;
    let mut nf = 0;
    for (b, n) in bm.range_iter() {
        if b {
            nt += n;
        } else {
            nf += n;
        }
    }
    assert_eq!(bm.len(), nt + nf);
}

criterion_group!(bench_range_iter_group, bench_range_iter);
criterion_main!(bench_range_iter_group);
