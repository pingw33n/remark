use criterion::*;

use byteorder::BigEndian;
use remark_log::bytes::*;
use parking_lot::Mutex;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
//    c.bench_function("Cursor read", |b| {
//        let mut v = Cursor::new(Bytes::new());
//        v.write_u8(1);
//        v.write_u16::<BigEndian>(2);
//        v.write_u32::<BigEndian>(3);
//        v.write_u64::<BigEndian>(4);
//
//        b.iter(|| {
//            v.set_position(0);
//            v.read_u8() as u64 +
//                v.read_u16::<BigEndian>() as u64 +
//                v.read_u32::<BigEndian>() as u64 +
//                v.read_u64::<BigEndian>()
//        })
//    });
//    c.bench_function("io::Cursor<Vec<u8>> read", |b| {
//        use byteorder::{ReadBytesExt, WriteBytesExt};
//        use std::io::Cursor;
//        let mut v = Cursor::new(Vec::new());
//        v.write_u8(1).unwrap();
//        v.write_u16::<BigEndian>(2).unwrap();
//        v.write_u32::<BigEndian>(3).unwrap();
//        v.write_u64::<BigEndian>(4).unwrap();
//
//        b.iter(|| {
//            v.set_position(0);
//            v.read_u8().unwrap() as u64 +
//                v.read_u16::<BigEndian>().unwrap() as u64 +
//                v.read_u32::<BigEndian>().unwrap() as u64 +
//                v.read_u64::<BigEndian>().unwrap()
//        })
//    });
//    c.bench_function("Bytes::get_u32()", |b| {
//        let v = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
//        b.iter(|| v.get_u32::<BigEndian>(3))
//    });
////    c.bench_function("BytesMut::get_u32()", |b| {
////        let v = bytes::BytesMut::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
////        b.iter(|| BigEndian::read_u32(&v[3..]))
////    });
//    c.bench_function("Bytes::get()", |b| {
//        let v = Bytes::with_len(100);
//        b.iter(|| v.get(50..75))
//    });
//    c.bench_function("Bytes::get().get_u32()", |b| {
//        let v = Bytes::with_len(100);
//        b.iter(|| v.get(50..75).get_u32::<BigEndian>(10))
//    });
//    c.bench_function("BigEndian::read_u32 on vec slice", |b| {
//        let v = vec![0; 100];
//        b.iter(|| BigEndian::read_u32(&v[10..]))
//    });
//    c.bench_function("vec with mutex", |b| {
//        let v = Mutex::new(vec![0; 100]);
//        b.iter(|| { let vv = v.lock(); BigEndian::read_u32(&vv[10..]); })
//    });
//    c.bench_function("vec with arc mutex", |b| {
//        let v = Arc::new(Mutex::new(vec![0; 100]));
//        b.iter(|| { let vv = v.lock(); BigEndian::read_u32(&vv[10..]); })
//    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);