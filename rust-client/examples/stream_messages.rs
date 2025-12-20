use rockserver_client::{RockserverClient, ColumnSchema};
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the specified server
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
    while let Some(result) = stream.next().await {
        match result {
            Ok(kv) => {
                println!("Message: Key={:?}, Value={:?}", kv.keys, kv.value);
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
