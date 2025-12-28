use std::env;
use std::path::Path;
use std::fs;
use rust_data_generator::schema::Schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let yaml_path = "schema/database-records.yaml";
    println!("cargo:rerun-if-changed={}", yaml_path);

    let mut yaml_content = fs::read_to_string(yaml_path)?;
    
    // Fix potential YAML formatting issues (missing spaces in flow style)
    yaml_content = yaml_content.replace("{data:{}}", "{ data: {} }");
    yaml_content = yaml_content.replace("{data: {}}", "{ data: {} }");
    yaml_content = yaml_content.replace("{data:{ }}", "{ data: {} }");

    let schema: Schema = serde_yaml::from_str(&yaml_content)?;

    let out_dir = env::var("OUT_DIR")?;
    let dest_dir = Path::new(&out_dir).join("generated_records");
    
    rust_data_generator::generator::generate(&schema, &dest_dir)?;

    Ok(())
}
