use criterion::{criterion_group, criterion_main, Criterion};
use pprof::criterion::{PProfProfiler, Output};
use doradb_compute::BinaryEval;
use doradb_compute::arith::{Impl, AddI32};
use doradb_storage::attr::Attr;
use doradb_datatype::PreciseType;

fn bench_add(c: &mut Criterion) {
    let size = 4096;
    let c1 = Attr::from((0..size as i32).into_iter());
        let c2 = Attr::from((0..size as i32).into_iter());
        c.bench_function(&format!("flat_codec_add_{}", size), |b| {
            b.iter(|| {
                let add = Impl(AddI32);
                let _ = add.binary_eval(PreciseType::i32(), &c1, &c2, None).unwrap();
            })
        });
}

criterion_group!(
    name = bench_add_group;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench_add);
criterion_main!(bench_add_group);
