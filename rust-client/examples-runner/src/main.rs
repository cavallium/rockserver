use rockserver_client::{RockserverClient, ColumnSchema};
use rust_data_generator::stream::{SafeDataInputStream, SafeDataInput};
use rust_data_generator::datagen::DataSerializer;
use futures::StreamExt;
use std::io::Cursor;
use std::convert::TryInto;
use std::io::Read;
use std::io as std_io;
use std::env;
use std::time::{Instant, Duration};

pub mod records {
    include!(concat!(env!("OUT_DIR"), "/generated_records/mod.rs"));
}
use records::v0_0_13::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let errors_only = args.contains(&"--errors-only".to_string());

    let client = RockserverClient::connect("http://10.0.0.11:5333").await?;

    // Define the schema based on the Java snippet:
    // CHAT_ENTITY_ID_BYTES (8) + Integer.BYTES (4)
    // No variable tail keys
    // Has value
    let schema = ColumnSchema {
        fixed_keys: vec![8, 4],
        variable_tail_keys: vec![],
        has_value: true,
        merge_operator_name: None,
        merge_operator_version: None,
    };

    let column_name = "messages".to_string();

    // Try to create the column. If it fails (e.g. already exists), get its ID.
    // Note: In a robust application, you'd check the error type.
    let column_id = match client.create_column(column_name.clone(), schema).await {
        Ok(id) => {
            println!("Created column '{}' with ID: {}", column_name, id);
            id
        },
        Err(e) => {
            eprintln!("Failed to create column (might exist), trying to get ID. Error: {}", e);
            client.get_column_id(column_name.clone()).await?
        }
    };
    
    println!("Using Column ID: {}", column_id);

    // Open a transaction for consistent reading
    let tx_timeout_ms = 60_000;
    let tx_id = client.open_transaction(tx_timeout_ms).await?;

    // Stream all messages
    // Passing empty vectors for keys implies scanning the full range.
    let mut stream = client.get_all_in_range(
        tx_id,
        column_id,
        vec![], // start_keys_inclusive
        vec![], // end_keys_exclusive
        false,  // reverse
        tx_timeout_ms,
    ).await?;

    println!("Streaming messages...");
    let mut total_count = 0;
    let mut error_count = 0;
    let start_time = Instant::now();
    let mut last_log_time = Instant::now();
    let log_interval = Duration::from_secs(2);

    while let Some(result) = stream.next().await {
        total_count += 1;
        match result {
            Ok(kv) => {
                // Decode ChatEntityId (Key Part 1)
                let chat_entity_id = if let Some(bytes) = kv.keys.get(0) {
                    let mut cursor = Cursor::new(bytes);
                    let mut input = SafeDataInputStream::new(&mut cursor);
                    match ChatEntityIdSerializer.deserialize(&mut input) {
                        Ok(id) => format!("{:?}", id),
                        Err(e) => format!("DecodeError({:?})", e),
                    }
                } else {
                    "Missing".to_string()
                };

                // Decode Message ID (Key Part 2)
                let message_id = if let Some(bytes) = kv.keys.get(1) {
                    if bytes.len() == 4 {
                        let val = i32::from_be_bytes(bytes.as_slice().try_into().unwrap());
                        format!("{}", val)
                    } else {
                        format!("InvalidLen({})", bytes.len())
                    }
                } else {
                    "Missing".to_string()
                };

                // Decode Message (Value)
                let message_display = if !kv.value.is_empty() {
                    let mut cursor = Cursor::new(&kv.value);
                    
                    // Read Version Byte
                    let mut version_buf = [0u8; 1];
                    if let Ok(_) = cursor.read_exact(&mut version_buf) {
                        let version = version_buf[0];
                        
                        let mut input = SafeDataInputStream::new(&mut cursor);
                        
                        // Dispatch based on version
                        // Note: We use the serializers from generated versions, which all produce the latest ImportedMessage type.
                        let decoded_result = match version {
                            0 => records::v0_0_0::ImportedMessageSerializer.deserialize(&mut input),
                            1 => records::v0_0_1::ImportedMessageSerializer.deserialize(&mut input),
                            2 => records::v0_0_2::ImportedMessageSerializer.deserialize(&mut input),
                            3 => records::v0_0_3::ImportedMessageSerializer.deserialize(&mut input),
                            4 => records::v0_0_4::ImportedMessageSerializer.deserialize(&mut input),
                            5 => records::v0_0_5::ImportedMessageSerializer.deserialize(&mut input),
                            6 => records::v0_0_6::ImportedMessageSerializer.deserialize(&mut input),
                            7 => records::v0_0_7::ImportedMessageSerializer.deserialize(&mut input),
                            8 => records::v0_0_8::ImportedMessageSerializer.deserialize(&mut input),
                            9 => records::v0_0_9::ImportedMessageSerializer.deserialize(&mut input),
                            10 => records::v0_0_10::ImportedMessageSerializer.deserialize(&mut input),
                            11 => records::v0_0_11::ImportedMessageSerializer.deserialize(&mut input),
                            12 => records::v0_0_12::ImportedMessageSerializer.deserialize(&mut input),
                            13 => records::v0_0_13::ImportedMessageSerializer.deserialize(&mut input),
                            _ => Err(std_io::Error::new(std_io::ErrorKind::InvalidData, format!("Unknown version {}", version))),
                        };

                        match decoded_result {
                            Ok(imported_msg) => {
                                let msg = imported_msg.value;
                                match &msg.content {
                                    MessageContent::MessageText(text) => {
                                        format!("Text: {:?} (Ver {})", text.text.text, version)
                                    },
                                    MessageContent::MessageEmpty(_) => format!("Empty (Ver {})", version),
                                    _ => format!("{:?} (Ver {})", msg.content, version),
                                }
                            },
                            Err(e) => format!("DecodeError: {} (Ver {})", e, version),
                        }
                    } else {
                         "EmptyStream".to_string()
                    }
                } else {
                    "EmptyValue".to_string()
                };

                let has_error = chat_entity_id.contains("DecodeError") || message_display.contains("DecodeError");
                if has_error {
                    error_count += 1;
                }

                if !errors_only || has_error {
                    println!("Chat: {}, MsgID: {} -> Content: {}", chat_entity_id, message_id, message_display);
                }

                if errors_only && last_log_time.elapsed() >= log_interval {
                    let elapsed = start_time.elapsed();
                    let throughput = total_count as f64 / elapsed.as_secs_f64();
                    println!("Progress: {} messages. Time: {:.2}s. Speed: {:.2} msg/s. Errors: {}", 
                        total_count, elapsed.as_secs_f64(), throughput, error_count);
                    last_log_time = Instant::now();
                }
            }
            Err(e) => {
                eprintln!("Error reading stream: {}", e);
                break;
            }
        }
    }

    client.close_transaction(tx_id, false, 1000).await?;

    Ok(())
}
