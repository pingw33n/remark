fn main() {
    prost_build::compile_protos(&["src/proto/root.proto"],
                                &["src/proto"]).unwrap();
}