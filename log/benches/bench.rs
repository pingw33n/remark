use criterion::*;

fn bench(c: &mut Criterion) {
    c.bench_function("threaded_hot_index_lookup", |b| {
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

    fn create_compressed_entry(codec: remark_log::entry::Codec) -> (remark_log::entry::BufEntry,
            remark_log::bytes::BytesMut) {
        use remark_log::entry::BufEntryBuilder;
        use remark_log::message::*;

        let msgs: Vec<_> = (1..100)
            .map(|i| MessageBuilder {
                id: Some(Id::new(i)),
                timestamp: Some(Timestamp::now()),
                key: Some("key1".into()),
                value: Some("value1".into()),
                ..Default::default()
            })
            .collect();

        let mut eb = BufEntryBuilder::dense();
        eb.compression(codec).messages(msgs);
        eb.build()
    }

    c.bench_function("uncompressed_entry_iter", |b| {
        let (e, buf) = create_compressed_entry(remark_log::entry::Codec::Uncompressed);
        b.iter(|| {
            e.iter(&buf).map(|v| v.unwrap()).collect::<Vec<_>>()
        });
    });
    c.bench_function("lz4_entry_iter", |b| {
        let (e, buf) = create_compressed_entry(remark_log::entry::Codec::Lz4);
        b.iter(|| {
            e.iter(&buf).map(|v| v.unwrap()).collect::<Vec<_>>()
        });
    });
    c.bench_function("zstd_entry_iter", |b| {
        let (e, buf) = create_compressed_entry(remark_log::entry::Codec::Zstd);
        b.iter(|| {
            e.iter(&buf).map(|v| v.unwrap()).collect::<Vec<_>>()
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);