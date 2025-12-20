use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{thread_rng, Rng};
use doradb_storage::bitmap::Bitmap;

fn bench_count(c: &mut Criterion) {
    const N1: usize = 10240;
    const N2: usize = 10238;
    const N3: usize = 10242;
    let mut bm1 = Bitmap::with_capacity(N1);
    let mut bm2 = Bitmap::with_capacity(N2);
    let mut bm3 = Bitmap::with_capacity(N3);

    let mut thd_rng = thread_rng();
    for _ in 0..N1 {
        bm1.add(thd_rng.gen());
    }
    for _ in 0..N2 {
        bm2.add(thd_rng.gen());
    }
    for _ in 0..N3 {
        bm3.add(thd_rng.gen());
    }

    c.bench_function("bitmap_count_false_10240", |b| {
        b.iter(|| black_box(count_false(&mut bm1)))
    });
    c.bench_function("bitmap_count_false_10238", |b| {
        b.iter(|| black_box(count_false(&mut bm2)))
    });
    c.bench_function("bitmap_count_false_10242", |b| {
        b.iter(|| black_box(count_false(&mut bm3)))
    });
}

fn count_false(bm: &Bitmap) -> usize {
    bm.false_count()
}

criterion_group!(bench_count_group, bench_count);
criterion_main!(bench_count_group);
