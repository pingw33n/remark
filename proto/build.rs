fn main() {
    prost_build::compile_protos(&["src/proto/proto.proto"],
                                &["src/proto/"]).unwrap();
}