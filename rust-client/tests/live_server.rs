//! Against a running server, named by `ROCKSERVER_TEST_ADDR` (for example `http://127.0.0.1:5333`); skipped when it
//! is unset. The Java test `RustClientLiveTest` (distributed module) starts a three-node in-process cluster and runs
//! this test against a follower, so that numbered writes also cross a forward. The server must load
//! `it.cavallium.rockserver.core.impl.MyStringAppendOperator` (the cluster's merge configuration), which joins merged
//! values with commas.

use futures::StreamExt;
use rockserver_client::proto::{Kv, KvBatch, MergeBatchMode, PutBatchMode};
use rockserver_client::{merge_operator_hash, ColumnSchema, RequestContext, RockserverClient};
use std::time::Duration;

const OPERATOR_CLASS: &str = "it.cavallium.rockserver.core.impl.MyStringAppendOperator";

fn key(value: u64) -> Vec<Vec<u8>> {
    vec![value.to_be_bytes().to_vec()]
}

fn schema() -> ColumnSchema {
    ColumnSchema {
        fixed_keys: vec![8],
        variable_tail_keys: vec![],
        has_value: true,
        merge_operator_name: None,
        merge_operator_version: None,
    }
}

/// An empty jar: the operator class is on the server's classpath.
fn empty_jar() -> Vec<u8> {
    // End of central directory record of an empty zip.
    let mut jar = vec![0x50, 0x4b, 0x05, 0x06];
    jar.extend_from_slice(&[0; 18]);
    jar
}

fn text(value: &Option<Vec<u8>>) -> Option<String> {
    value
        .as_ref()
        .map(|bytes| String::from_utf8(bytes.clone()).unwrap())
}

#[tokio::test]
async fn numbered_writes_against_a_live_server() {
    let Ok(address) = std::env::var("ROCKSERVER_TEST_ADDR") else {
        eprintln!("ROCKSERVER_TEST_ADDR is not set: skipped");
        return;
    };
    let client = RockserverClient::connect(address, RequestContext::batch_with_timeout(Duration::from_secs(30)))
        .await
        .expect("connect");
    assert!(client.client_sessions(), "a replicated server offers client sessions");

    // Schema changes, numbered: creating the same column again answers with its id.
    let column = client.create_column("rust-live".to_owned(), schema()).await.unwrap();
    assert_eq!(client.create_column("rust-live".to_owned(), schema()).await.unwrap(), column);
    let doomed = client.create_column("rust-doomed".to_owned(), schema()).await.unwrap();
    assert!(client.delete_column_if_exists("rust-doomed".to_owned()).await.unwrap());
    assert!(!client.delete_column_if_exists("rust-doomed".to_owned()).await.unwrap());
    assert_ne!(doomed, column);

    // Merge operators (CC-R3): upload, check, ensure.
    let jar = empty_jar();
    let version = client
        .upload_merge_operator("rust-op".to_owned(), OPERATOR_CLASS.to_owned(), jar.clone())
        .await
        .unwrap();
    assert_eq!(
        client
            .check_merge_operator("rust-op".to_owned(), merge_operator_hash(&jar))
            .await
            .unwrap(),
        Some(version)
    );
    assert_eq!(
        client
            .ensure_merge_operator("rust-op".to_owned(), OPERATOR_CLASS.to_owned(), jar.clone())
            .await
            .unwrap(),
        version
    );
    assert_eq!(
        client
            .check_merge_operator("rust-op".to_owned(), merge_operator_hash(b"no such jar"))
            .await
            .unwrap(),
        None
    );

    // Single writes, merges with results.
    client.put(0, column, key(1), b"a".to_vec()).await.unwrap();
    client.merge(0, column, key(1), b"b".to_vec()).await.unwrap();
    assert_eq!(
        text(&client.merge_get_merged(0, column, key(1), b"c".to_vec()).await.unwrap()).as_deref(),
        Some("a,b,c")
    );
    assert_eq!(
        text(&client.put_get_previous(0, column, key(2), b"x".to_vec()).await.unwrap()),
        None
    );
    assert!(client.delete_get_previous_presence(0, column, key(2)).await.unwrap());

    // Streamed multi writes of a BATCH caller: every item applied (one numbered write each call).
    client
        .merge_multi(
            0,
            column,
            futures::stream::iter((10..13u64).map(|k| Kv {
                keys: key(k),
                value: format!("m{k}").into_bytes(),
            })),
        )
        .await
        .unwrap();
    let previous: Vec<_> = client
        .put_multi_get_previous(
            0,
            column,
            futures::stream::iter([12u64, 13].map(|k| Kv {
                keys: key(k),
                value: format!("p{k}").into_bytes(),
            })),
        )
        .await
        .unwrap()
        .collect()
        .await;
    let previous: Vec<_> = previous.into_iter().map(|p| p.unwrap().previous).collect();
    assert_eq!(previous, vec![Some(b"m12".to_vec()), None]);
    for (k, expected) in [(10u64, "m10"), (11, "m11"), (12, "p12"), (13, "p13")] {
        assert_eq!(
            text(&client.get(0, column, key(k)).await.unwrap()).as_deref(),
            Some(expected),
            "key {k}"
        );
    }
    let presence: Vec<bool> = client
        .delete_multi_get_previous_presence(0, column, futures::stream::iter([key(10), key(99)]))
        .await
        .unwrap()
        .map(|p| p.unwrap().present)
        .collect()
        .await;
    assert_eq!(presence, vec![true, false]);
    client
        .put_multi_list(
            0,
            column,
            vec![Kv { keys: key(20), value: b"l".to_vec() }],
        )
        .await
        .unwrap();

    // An update: its final write is numbered.
    let update = client.get_for_update(0, column, key(30)).await.unwrap();
    let update_id = update.update_id.expect("an update id");
    client.put(update_id, column, key(30), b"u".to_vec()).await.unwrap();
    assert_eq!(text(&client.get(0, column, key(30)).await.unwrap()).as_deref(), Some("u"));

    // A transaction: staged writes unnumbered, the commit numbered.
    let tx = client.open_transaction(Duration::from_secs(60)).await.unwrap();
    client.put(tx, column, key(40), b"t".to_vec()).await.unwrap();
    assert!(client.close_transaction(tx, true).await.unwrap());
    assert_eq!(text(&client.get(0, column, key(40)).await.unwrap()).as_deref(), Some("t"));

    // Batches as numbered chunks: five values of which two fill a chunk, three chunks.
    let boundary = (rockserver_client::sessions::CHUNK_BYTES / 2
        - 8
        - 2 * rockserver_client::sessions::ITEM_OVERHEAD_BYTES) as usize;
    let batch = KvBatch {
        entries: (50..55u64)
            .map(|k| Kv {
                keys: key(k),
                value: vec![b'b'; boundary],
            })
            .collect(),
    };
    client
        .put_batch(column, PutBatchMode::WriteBatch, futures::stream::iter([batch]))
        .await
        .unwrap();
    for k in 50..55u64 {
        assert_eq!(client.get(0, column, key(k)).await.unwrap().map(|v| v.len()), Some(boundary));
    }
    client
        .merge_batch(
            column,
            MergeBatchMode::MergeWriteBatch,
            futures::stream::iter([KvBatch {
                entries: vec![Kv { keys: key(1), value: b"d".to_vec() }],
            }]),
        )
        .await
        .unwrap();
    assert_eq!(text(&client.get(0, column, key(1)).await.unwrap()).as_deref(), Some("a,b,c,d"));

    // A range deletion.
    client.delete_range(column, key(50), key(55)).await.unwrap();
    assert_eq!(client.get(0, column, key(50)).await.unwrap(), None);

    // Unnumbered writes still work.
    let plain = client.without_client_sessions();
    assert!(!plain.client_sessions());
    plain.put(0, column, key(60), b"plain".to_vec()).await.unwrap();
    assert_eq!(text(&client.get(0, column, key(60)).await.unwrap()).as_deref(), Some("plain"));
}
