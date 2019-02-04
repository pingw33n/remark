use criterion::*;

fn bench(c: &mut Criterion) {
    c.bench_function("Threaded hot index read", |b| {
        use remark_log::index::Mode;
        use remark_log::segment::IdIndex;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let f = mktemp::Temp::new_file().unwrap();
        let idx = Arc::new(IdIndex::open(&f, Mode::ReadWrite {
            preallocate: 1000,
            max_capacity: 1000,
        }).unwrap());

        for i in 0..1000 {
            idx.push(i, i * 1000).unwrap();
        }

        let flag = Arc::new(AtomicBool::new(false));

        for _ in 0..16 {
            let idx_ = idx.clone();
            let flag_ = flag.clone();
            std::thread::spawn(move || while !flag_.load(Ordering::Acquire) {
                for _ in 0..500 {
                    idx_.entry_by_key(888);
                }
            });
        }

        b.iter(|| {
            idx.entry_by_key(888)
        });

        flag.store(true, std::sync::atomic::Ordering::Release);
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);