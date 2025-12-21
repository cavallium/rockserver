use std::env;
use std::path::Path;
use std::fs;
use rust_data_generator::schema::Schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile proto
    tonic_build::compile_protos("proto/rocksdb.proto")?;

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

