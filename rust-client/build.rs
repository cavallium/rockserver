use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for name in ["rocksdb.proto", "exports.proto", "cdc.proto"] {
        let bundled = format!("proto/{name}");
        let canonical = format!("../src/main/proto/{name}");
        println!("cargo:rerun-if-changed={bundled}");
        println!("cargo:rerun-if-changed={canonical}");
        if Path::new(&canonical).exists() && fs::read(&bundled)? != fs::read(&canonical)? {
            return Err(format!("{bundled} must exactly match {canonical}").into());
        }
    }
    let bundled_proto = Path::new("proto/rocksdb.proto");
    // Compile proto
    // Find protobuf include path from Maven artifacts
    let proto_dep_dir = Path::new("../target/protoc-dependencies");

    let include_path = fs::read_dir(proto_dep_dir)
        .ok()
        .and_then(|entries| {
            entries.flatten().find_map(|entry| {
                let path = entry.path();
                if path.is_dir() && path.join("google/protobuf/empty.proto").exists() {
                    Some(path.to_string_lossy().into_owned())
                } else {
                    None
                }
            })
        })
        .ok_or("Could not find google/protobuf/empty.proto in ../target/protoc-dependencies. Ensure the Java project is built to generate dependencies.")?;

    tonic_build::configure().compile_protos(
        &[
            bundled_proto,
            Path::new("proto/exports.proto"),
            Path::new("proto/cdc.proto"),
        ],
        &["proto", &include_path],
    )?;

    Ok(())
}
