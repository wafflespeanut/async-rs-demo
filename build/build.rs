use std::env;
use std::path::PathBuf;

fn main() {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    tonic_build::compile_protos(root.join("build").join("orderbook.proto"))
        .expect("compiling protobuf");
}
