use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::Rng;
use doradb_datatype::memcmp::MemCmpFormat;

fn bench_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("memsort");
    for log2_size in [12, 14] {
        let size = 2usize.pow(log2_size);
        let mut rng = rand::thread_rng();
        let orig: Vec<u32> = (0..size).map(|_| rng.gen()).collect();
        let mut data1 = orig.clone();
        let mut data2 = vec![[0u8; 4]; size];
        group.bench_function(&format!("primitive_u32_{}", size), |b| {
            b.iter(black_box(|| {
                data1.copy_from_slice(&orig);
                data1.sort();
            }))
        });

        group.bench_function(&format!("memcmp_u32_{}", size), |b| {
            b.iter(black_box(|| {
                for (src, tgt) in orig.iter().zip(data2.iter_mut()) {
                    u32::write_mcf(src, tgt);
                }
                data2.sort();
            }))
        });

        // let mut buf = vec![0u8; 4];
        // for (d1, d2) in data1.iter().zip(data2) {
        //     u32::write_mcf(d1, &mut buf);
        //     assert_eq!(buf, d2);
        // }
    }
    group.finish()
}

criterion_group!(benches, bench_sort);
criterion_main!(benches);
