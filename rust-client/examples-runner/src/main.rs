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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::fs::File;
use std::io::Write;

pub mod records {
    include!(concat!(env!("OUT_DIR"), "/generated_records/mod.rs"));
}
use records::v0_0_13::*;
mod query;
use query::{Query, MessageContext, parse_query};
use serde_json::json;

fn get_chat_entity_id_value(id: &ChatEntityId) -> i64 {
    match id {
        ChatEntityId::UserId(u) => u.table_id.0.abs(),
        ChatEntityId::BasicGroupId(g) => -g.table_id.0.abs(),
        ChatEntityId::SuperCollectionId(s) => -1000000000000 - s.table_id.0.abs(),
        ChatEntityId::DeprecatedId(d) => d.table_id.0.abs(),
        ChatEntityId::MonoforumId(m) => -1000000000000 - m.table_id.0.abs(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let errors_only = args.contains(&"--errors-only".to_string());
    let approx_total_count: Option<u64> = env::var("APPROX_TOTAL_COUNT")
        .ok()
        .and_then(|v| v.parse().ok());

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

    // Stream all messages using scanRaw
    // (Stream creation moved to parallel shards below)

    println!("Streaming messages...");
    let total_count = Arc::new(AtomicU64::new(0));
    let error_count = Arc::new(AtomicU64::new(0));
    let match_count = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    // Logger task
    let total_count_log = total_count.clone();
    let error_count_log = error_count.clone();
    let match_count_log = match_count.clone();
    let approx_total_count_log = approx_total_count;
    
    tokio::spawn(async move {
        let mut last_log_time = Instant::now();
        let mut last_log_count = 0;
        let log_interval = Duration::from_secs(2);
        
        loop {
            tokio::time::sleep(log_interval).await;
            let current_total = total_count_log.load(Ordering::Relaxed);
            let current_errors = error_count_log.load(Ordering::Relaxed);
            let current_matches = match_count_log.load(Ordering::Relaxed);
            let elapsed = start_time.elapsed();
            let interval_elapsed = last_log_time.elapsed();
            let interval_count = current_total - last_log_count;
            
            let current_speed = interval_count as f64 / interval_elapsed.as_secs_f64();
            let total_speed = current_total as f64 / elapsed.as_secs_f64();

            let mut log_msg = format!("Progress: {} messages", current_total);

            if let Some(total) = approx_total_count_log {
                let percentage = if total > 0 { (current_total as f64 / total as f64) * 100.0 } else { 0.0 };
                log_msg.push_str(&format!(" ({:.2}%)", percentage));
            }

            log_msg.push_str(&format!(". Time: {:.2}s.", elapsed.as_secs_f64()));

            if let Some(total) = approx_total_count_log {
                let remaining = if total > current_total { total - current_total } else { 0 };
                let eta_seconds = if total_speed > 0.0 { remaining as f64 / total_speed } else { 0.0 };
                log_msg.push_str(&format!(" ETA: {}.", format_duration(eta_seconds)));
            }

            let match_percent = if current_total > 0 { (current_matches as f64 / current_total as f64) * 100.0 } else { 0.0 };
            log_msg.push_str(&format!(" Current Speed: {:.2} msg/s. Total Speed: {:.2} msg/s. Errors: {}. Matches: {} ({:.3}%)", 
                current_speed, total_speed, current_errors, current_matches, match_percent));

            println!("{}", log_msg);
            last_log_time = Instant::now();
            last_log_count = current_total;
        }
    });

    let parallelism = thread::available_parallelism().map(|p| p.get()).unwrap_or(4);
    let shard_count = env::var("SHARD_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let shard_parallelism = usize::max(1, parallelism / shard_count);

    // Parse search query
    let search_query_str = env::var("SEARCH_QUERY")
        .unwrap_or_else(|_| "(text:\"il movente\" || text:\"mari maruso\") && (text:\"mari_maruso\" || text:\"il_movente\" || text:\"maruso\")".to_string());
    let search_query = parse_query(&search_query_str).expect("Invalid SEARCH_QUERY");
    let search_query = Arc::new(search_query);

    // Output file setup
    let output_file_path = env::var("OUTPUT_FILE").ok();
    let match_tx = if let Some(path) = output_file_path {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let path = path.clone();
        let rt = tokio::runtime::Handle::current();
        std::thread::spawn(move || {
            let file = File::create(path).expect("Failed to create output file");
            let mut writer = std::io::BufWriter::new(file);
            rt.block_on(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        res = rx.recv() => {
                            match res {
                                Some(line) => {
                                    if let Err(e) = writeln!(writer, "{}", line) {
                                        eprintln!("Failed to write to file: {}", e);
                                    }
                                }
                                None => break,
                            }
                        }
                        _ = interval.tick() => {
                             if let Err(e) = writer.flush() {
                                 eprintln!("Failed to flush file: {}", e);
                             }
                        }
                    }
                }
                let _ = writer.flush();
            });
        });
        Some(tx)
    } else {
        None
    };

    let mut handles = Vec::new();
    for shard_index in 0..shard_count {
        let client = client.clone();
        let column_id = column_id;
        let total_count = total_count.clone();
        let error_count = error_count.clone();
        let match_count = match_count.clone();
        let errors_only = errors_only;
        let search_query = search_query.clone();
        let match_tx = match_tx.clone();

        handles.push(tokio::spawn(async move {
            match client.scan_raw(column_id, shard_index as i32, shard_count as i32).await {
                Ok(stream) => {
                    stream.for_each_concurrent(Some(shard_parallelism), |batch_res| {
                        let total_count = total_count.clone();
                        let error_count = error_count.clone();
                        let match_count = match_count.clone();
                        let search_query = search_query.clone();
                        let match_tx = match_tx.clone();
                        async move {
                            match batch_res {
                                Ok(batch) => {
                                    tokio::task::spawn_blocking(move || {
                                        let mut batch_count = 0;
                                        let mut batch_errors = 0;
                                        let mut batch_matches = 0;
                                        for kv in batch.entries {
                                            batch_count += 1;
                                            
                                            // Decode ChatEntityId (Key Part 1)
                                            let chat_entity_id_res = if let Some(bytes) = kv.keys.get(0) {
                                                let mut cursor = Cursor::new(bytes);
                                                let mut input = SafeDataInputStream::new(&mut cursor);
                                                ChatEntityIdSerializer.deserialize(&mut input)
                                            } else {
                                                Err(std_io::Error::new(std_io::ErrorKind::InvalidData, "Missing"))
                                            };

                                            // Decode Message ID (Key Part 2)
                                            let message_id_res = if let Some(bytes) = kv.keys.get(1) {
                                                if bytes.len() == 4 {
                                                    Ok(i32::from_be_bytes(bytes.as_slice().try_into().unwrap()))
                                                } else {
                                                    Err(std_io::Error::new(std_io::ErrorKind::InvalidData, format!("InvalidLen({})", bytes.len())))
                                                }
                                            } else {
                                                Err(std_io::Error::new(std_io::ErrorKind::InvalidData, "Missing"))
                                            };
                                            
                                            // Decode Message (Value)
                                            let mut version = 0;
                                            let message_res = if !kv.value.is_empty() {
                                                let mut cursor = Cursor::new(&kv.value);
                                                let mut version_buf = [0u8; 1];
                                                if let Ok(_) = cursor.read_exact(&mut version_buf) {
                                                    version = version_buf[0];
                                                    let mut input = SafeDataInputStream::new(&mut cursor);
                                                    match version {
                                                        0 => records::v0_0_0::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        1 => records::v0_0_1::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        2 => records::v0_0_2::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        3 => records::v0_0_3::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        4 => records::v0_0_4::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        5 => records::v0_0_5::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        6 => records::v0_0_6::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        7 => records::v0_0_7::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        8 => records::v0_0_8::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        9 => records::v0_0_9::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        10 => records::v0_0_10::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        11 => records::v0_0_11::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        12 => records::v0_0_12::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        13 => records::v0_0_13::ImportedMessageSerializer.deserialize(&mut input).map(|m| m.value),
                                                        _ => Err(std_io::Error::new(std_io::ErrorKind::InvalidData, format!("Unknown version {}", version))),
                                                    }
                                                } else {
                                                    Err(std_io::Error::new(std_io::ErrorKind::UnexpectedEof, "EmptyStream"))
                                                }
                                            } else {
                                                Err(std_io::Error::new(std_io::ErrorKind::InvalidData, "EmptyValue"))
                                            };

                                            let is_missing_key0 = matches!(&chat_entity_id_res, Err(e) if e.to_string() == "Missing");
                                            let is_empty_val = matches!(&message_res, Err(e) if e.to_string() == "EmptyValue" || e.to_string() == "EmptyStream");
                                            
                                            let has_error = (!is_missing_key0 && chat_entity_id_res.is_err()) || (!is_empty_val && message_res.is_err());
                                            
                                            if has_error {
                                                batch_errors += 1;
                                                if let Err(e) = &chat_entity_id_res {
                                                    if !is_missing_key0 {
                                                        eprintln!("Error decoding Key0: {}", e);
                                                    }
                                                }
                                                if let Err(e) = &message_res {
                                                    if !is_empty_val {
                                                        eprintln!("Error decoding Value: {}", e);
                                                    }
                                                }
                                            }

                                            if !errors_only || has_error {
                                                let mut matched = false;
                                                let mut text_parts: Vec<&str> = Vec::new();
                                                let mut sender_id_val: Option<i64> = None;
                                                let mut chat_id_val: Option<i64> = None;
                                                let mut key_chat_id: Option<i64> = None;
                                                let mut key_message_id: Option<i64> = None;
                                                let mut val_chat_id: Option<i64> = None;
                                                let mut val_message_id: Option<i64> = None;

                                                if let Ok(id) = &chat_entity_id_res {
                                                    let cid = get_chat_entity_id_value(id);
                                                    chat_id_val = Some(cid);
                                                    key_chat_id = Some(cid);
                                                }
                                                
                                                if let Ok(id) = &message_id_res {
                                                    key_message_id = Some(*id as i64);
                                                }
                                                
                                                if let Ok(msg) = &message_res {
                                                    val_chat_id = Some(get_chat_entity_id_value(&msg.chat_entity_id));
                                                    val_message_id = Some(msg.id);
                                                    sender_id_val = Some(get_chat_entity_id_value(&msg.sender_id));
                                                    
                                                    match &msg.content {
                                                        MessageContent::MessageText(m) => {
                                                            text_parts.push(&m.text.text);
                                                            if let Some(preview) = &m.link_preview {
                                                                text_parts.push(&preview.url);
                                                                if let Some(d) = &preview.display_url { text_parts.push(d); }
                                                                if let Some(s) = &preview.site_name { text_parts.push(s); }
                                                                if let Some(t) = &preview.title { text_parts.push(t); }
                                                                if let Some(desc) = &preview.description { text_parts.push(&desc.text); }
                                                            }
                                                        },
                                                        MessageContent::MessageDocument(m) => {
                                                            if let Some(cap) = &m.caption { text_parts.push(&cap.text); }
                                                            if let Some(name) = &m.document.file_name { text_parts.push(name); }
                                                        },
                                                        MessageContent::MessagePhoto(m) => {
                                                            if let Some(cap) = &m.caption { text_parts.push(&cap.text); }
                                                        },
                                                        MessageContent::MessageVideo(m) => {
                                                            if let Some(cap) = &m.caption { text_parts.push(&cap.text); }
                                                            if let Some(name) = &m.video.file_name { text_parts.push(name); }
                                                        },
                                                        MessageContent::MessageAnimation(m) => {
                                                            if let Some(cap) = &m.caption { text_parts.push(&cap.text); }
                                                            if let Some(name) = &m.animation.file_name { text_parts.push(name); }
                                                        },
                                                        MessageContent::MessageAudio(m) => {
                                                            if let Some(cap) = &m.caption { text_parts.push(&cap.text); }
                                                            if let Some(name) = &m.audio.file_name { text_parts.push(name); }
                                                            if let Some(performer) = &m.audio.performer { text_parts.push(performer); }
                                                            if let Some(title) = &m.audio.title { text_parts.push(title); }
                                                        },
                                                        MessageContent::MessageVoiceNote(m) => {
                                                            if let Some(cap) = &m.caption { text_parts.push(&cap.text); }
                                                        },
                                                        MessageContent::MessageChatChangeTitle(m) => {
                                                            text_parts.push(&m.text);
                                                        },
                                                        MessageContent::MessageChatCreate(m) => {
                                                            text_parts.push(&m.title);
                                                        },
                                                        MessageContent::MessageContact(m) => {
                                                            if let Some(v) = &m.contact.first_name { text_parts.push(v); }
                                                            if let Some(v) = &m.contact.last_name { text_parts.push(v); }
                                                            if let Some(v) = &m.contact.phone_number { text_parts.push(v); }
                                                            if let Some(v) = &m.contact.vcard { text_parts.push(v); }
                                                        },
                                                        MessageContent::MessageCustomServiceAction(m) => {
                                                            text_parts.push(&m.text);
                                                        },
                                                        MessageContent::MessageGame(m) => {
                                                            text_parts.push(&m.game.title);
                                                            text_parts.push(&m.game.short_name);
                                                            if let Some(desc) = &m.game.description { text_parts.push(desc); }
                                                            if let Some(text) = &m.game.text { text_parts.push(&text.text); }
                                                        },
                                                        MessageContent::MessageInvoice(m) => {
                                                            text_parts.push(&m.product_info.title);
                                                            text_parts.push(&m.product_info.description.text);
                                                        },
                                                        MessageContent::MessagePoll(m) => {
                                                            text_parts.push(&m.poll.question.text);
                                                            for opt in &m.poll.options {
                                                                text_parts.push(&opt.text.text);
                                                            }
                                                        },
                                                        MessageContent::MessageSticker(m) => {
                                                            if let Some(emoji) = &m.sticker.emoji { text_parts.push(emoji); }
                                                        },
                                                        MessageContent::MessageVenue(m) => {
                                                            if let Some(title) = &m.venue.title { text_parts.push(title); }
                                                            if let Some(addr) = &m.venue.address { text_parts.push(addr); }
                                                            if let Some(prov) = &m.venue.provider { text_parts.push(prov); }
                                                            if let Some(typ) = &m.venue.r#type { text_parts.push(typ); }
                                                        },
                                                        MessageContent::MessageForumTopicCreated(m) => {
                                                            text_parts.push(&m.name);
                                                        },
                                                        MessageContent::MessageForumTopicEdited(m) => {
                                                            if let Some(name) = &m.name { text_parts.push(name); }
                                                        },
                                                        _ => {}
                                                    }
                                                }

                                                let ctx = MessageContext {
                                                    text: text_parts,
                                                    chat_id: chat_id_val,
                                                    sender_id: sender_id_val,
                                                    key_chat_id,
                                                    key_message_id,
                                                    val_chat_id,
                                                    val_message_id,
                                                };
                                                
                                                matched = search_query.matches(&ctx);
                                                
                                                if matched {
                                                    batch_matches += 1;

                                                    if let Some(tx) = &match_tx {
                                                        let value_obj = message_res.as_ref().ok();

                                                        let json_output = json!({
                                                            "key": {
                                                                "chatId": key_chat_id,
                                                                "messageId": key_message_id
                                                            },
                                                            "value": value_obj,
                                                            "serializedVersion": version
                                                        });
                                                        
                                                        let log_line = serde_json::to_string(&json_output).unwrap();
                                                        let _ = tx.send(log_line);
                                                    } else {
                                                        println!("Match found | ChatId: {} | MessageId: {} | Text: {:?}", 
                                                            key_chat_id.unwrap_or(0), 
                                                            key_message_id.unwrap_or(0), 
                                                            ctx.text
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        total_count.fetch_add(batch_count, Ordering::Relaxed);
                                        error_count.fetch_add(batch_errors, Ordering::Relaxed);
                                        match_count.fetch_add(batch_matches, Ordering::Relaxed);
                                    }).await.unwrap();
                                }
                                Err(e) => {
                                    eprintln!("Error reading stream: {}", e);
                                    // Don't break, just log error, stream might recover or end
                                }
                            }
                        }
                    }).await;
                }
                Err(e) => {
                    eprintln!("Failed to start scan for shard {}: {}", shard_index, e);
                }
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    println!("Computation finished successfully.");

    Ok(())
}

fn format_duration(seconds: f64) -> String {
    if seconds < 60.0 {
        return format!("{:.0}s", seconds);
    }
    let minutes = seconds / 60.0;
    if minutes < 60.0 {
        return format!("{:.1}m", minutes);
    }
    let hours = minutes / 60.0;
    if hours < 24.0 {
        return format!("{:.1}h", hours);
    }
    let days = hours / 24.0;
    if days < 7.0 {
        return format!("{:.1}d", days);
    }
    let weeks = days / 7.0;
    if days < 30.0 {
        return format!("{:.1}w", weeks);
    }
    let months = days / 30.0;
    if days < 365.0 {
        return format!("{:.1}mo", months);
    }
    let years = days / 365.0;
    format!("{:.1}y", years)
}
