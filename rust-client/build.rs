use std::path::Path;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bundled_proto = Path::new("proto/rocksdb.proto");
    let repository_proto = Path::new("../src/main/proto/rocksdb.proto");
    if repository_proto.exists() {
        let bundled = fs::read(bundled_proto)?;
        let canonical = fs::read(repository_proto)?;
        if bundled != canonical {
            return Err("rust-client/proto/rocksdb.proto must exactly match src/main/proto/rocksdb.proto".into());
        }
    }

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

    tonic_build::configure()
        .compile_protos(
            &[bundled_proto],
            &["proto", &include_path],
        )?;

    Ok(())
}
