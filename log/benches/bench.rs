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

    fn create_compressed_entry(msg_count: usize, codec: remark_log::entry::Codec)
        -> (remark_log::entry::BufEntry, Vec<u8>)
    {
        use remark_log::entry::BufEntryBuilder;
        use remark_log::message::*;

        let msgs: Vec<_> = (0..msg_count)
            .map(|i| MessageBuilder {
                id: Some(Id::new(i as u64)),
                timestamp: Some(Timestamp::now()),
                headers: Headers {
                    vec: vec![
                        Header { name: "header 1".into(), value: "value 1".into() },
                        Header { name: "header 2".into(), value: "value 2".into() },
                    ],
                },
                key: Some("key1".into()),
                value: Some("value1".into()),
                ..Default::default()
            })
            .collect();

        let mut eb = BufEntryBuilder::dense();
        eb.compression(codec).messages(msgs);
        eb.build()
    }

    c.bench_function("entry_iter_uncompressed", |b| {
        let (e, buf) = create_compressed_entry(1000, remark_log::entry::Codec::Uncompressed);
        b.iter(|| {
            e.iter(&buf).map(|v| v.unwrap()).for_each(|_| {})
        });
    });
    c.bench_function("entry_iter_lz4", |b| {
        let (e, buf) = create_compressed_entry(1000, remark_log::entry::Codec::Lz4);
        b.iter(|| {
            e.iter(&buf).map(|v| v.unwrap()).for_each(|_| {})
        });
    });
    c.bench_function("entry_iter_zstd", |b| {
        let (e, buf) = create_compressed_entry(1000, remark_log::entry::Codec::Zstd);
        b.iter(|| {
            e.iter(&buf).map(|v| v.unwrap()).for_each(|_| {})
        });
    });
    c.bench_function("message_read_uncompressed", |b| {
        use remark_common::bytes::Cursor;
        use remark_log::entry::format;
        use remark_log::message::*;

        let (_, buf) = create_compressed_entry(1, remark_log::entry::Codec::Uncompressed);
        let mut cur = Cursor::new(&buf);
        b.iter(|| {
            cur.set_position(55 /* format::MESSAGES_START */);
            Message::read(&mut cur, Id::new(0), Timestamp::epoch()).unwrap();
        });
    });
    c.bench_function("buf_message_read_uncompressed", |b| {
        use remark_common::bytes::Cursor;
        use remark_log::entry::format;
        use remark_log::message::*;

        let (_, buf) = create_compressed_entry(1, remark_log::entry::Codec::Uncompressed);
        let mut cur = Cursor::new(&buf);
        let mut buf = Vec::new();
        b.iter(|| {
            cur.set_position(format::MESSAGES_START);
            buf.clear();
            BufMessage::read(&mut cur, &mut buf, Id::new(0), Timestamp::epoch()).unwrap();
        });
    });

    fn bench_validate_body(b: &mut Bencher, compression: remark_log::entry::Codec) {
        let (e, buf) = create_compressed_entry(1000, compression);
        b.iter(|| {
            e.validate_body(&buf, remark_log::entry::ValidBody {
                dense: true,
                without_timestamp: false,
                ..Default::default()
            }).unwrap();
        });
    }
    c.bench_function("validate_body_uncompressed", |b| {
        bench_validate_body(b, remark_log::entry::Codec::Uncompressed);
    });
    c.bench_function("validate_body_lz4", |b| {
        bench_validate_body(b, remark_log::entry::Codec::Lz4);
    });
    c.bench_function("validate_body_zstd", |b| {
        bench_validate_body(b, remark_log::entry::Codec::Zstd);
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);