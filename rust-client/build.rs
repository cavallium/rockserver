use std::env;
use std::path::Path;
use std::fs;
use rust_data_generator::schema::Schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
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
            &["proto/rocksdb.proto"],
            &["proto", &include_path],
        )?;

    // Generate Datagen types using rust-data-generator
    let yaml_path = "examples/schema/database-records.yaml";
    println!("cargo:rerun-if-changed={}", yaml_path);

    let mut yaml_content = fs::read_to_string(yaml_path)?;
    
    // Fix potential YAML formatting issues (missing spaces in flow style)
    yaml_content = yaml_content.replace("{data:{}}", "{ data: {} }");
    yaml_content = yaml_content.replace("{data: {}}", "{ data: {} }");
    yaml_content = yaml_content.replace("{data:{ }}", "{ data: {} }");

    let schema: Schema = serde_yaml::from_str(&yaml_content)?;

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_dir = Path::new(&out_dir).join("generated_records");
    
    rust_data_generator::generator::generate(&schema, &dest_dir)?;

    Ok(())
}

