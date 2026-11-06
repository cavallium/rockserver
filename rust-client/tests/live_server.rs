//! Against a running server, named by `ROCKSERVER_TEST_ADDR` (for example `http://127.0.0.1:5333`). Ignored by a plain
//! `cargo test`, so that it shows as ignored rather than passing; run it with `-- --include-ignored`, which fails when
//! the variable is unset. The Java test `RustClientLiveTest` (distributed module) starts a three-node in-process cluster and runs
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
#[ignore = "needs a server at ROCKSERVER_TEST_ADDR (RustClientLiveTest runs it)"]
async fn numbered_writes_against_a_live_server() {
    let address = std::env::var("ROCKSERVER_TEST_ADDR")
        .expect("ROCKSERVER_TEST_ADDR names the server to test against");
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

    // A resend: two copies of one numbered merge (same session and sequence), as after an unknown outcome. The
    // second is answered from the server's record: merged once.
    let session = client.open_client_session().await.unwrap();
    let copy = client.with_context(
        RequestContext::batch_with_timeout(Duration::from_secs(30)).with_request_id(session, 0, 0),
    );
    copy.merge(0, column, key(70), b"once".to_vec()).await.unwrap();
    copy.merge(0, column, key(70), b"once".to_vec()).await.unwrap();
    assert_eq!(text(&client.get(0, column, key(70)).await.unwrap()).as_deref(), Some("once"));

    // A multi write over NUMBERED_MULTI_MAX_BYTES is streamed unnumbered, as before numbering: every item applied.
    let large = 1024 * 1024;
    client
        .put_multi(
            0,
            column,
            futures::stream::iter((80..86u64).map(move |k| Kv {
                keys: key(k),
                value: vec![b'L'; large],
            })),
        )
        .await
        .unwrap();
    for k in 80..86u64 {
        assert_eq!(client.get(0, column, key(k)).await.unwrap().map(|v| v.len()), Some(large), "key {k}");
    }

    // R6-6c: the variants with results over NUMBERED_MULTI_MAX_BYTES are streamed unnumbered too, with their results.
    let big = move |k: u64, fill: u8| Kv { keys: key(k), value: vec![fill; large] };
    client
        .put_multi_ensure(0, column, futures::stream::iter((90..95u64).map(move |k| big(k, b'e'))))
        .await
        .unwrap();
    let previous: Vec<_> = client
        .put_multi_get_previous(0, column, futures::stream::iter((90..95u64).map(move |k| big(k, b'p'))))
        .await
        .unwrap()
        .map(|p| p.unwrap().previous.map(|v| (v.len(), v[0])))
        .collect()
        .await;
    assert_eq!(previous, vec![Some((large, b'e')); 5]);
    let deltas: Vec<_> = client
        .put_multi_get_delta(0, column, futures::stream::iter((90..95u64).map(move |k| big(k, b'd'))))
        .await
        .unwrap()
        .map(|d| {
            let d = d.unwrap();
            (d.previous.map(|v| v[0]), d.current.map(|v| v[0]))
        })
        .collect()
        .await;
    assert_eq!(deltas, vec![(Some(b'p'), Some(b'd')); 5]);
    let changed: Vec<bool> = client
        .put_multi_get_changed(
            0,
            column,
            futures::stream::iter((90..95u64).map(move |k| big(k, if k < 92 { b'd' } else { b'c' }))),
        )
        .await
        .unwrap()
        .map(|c| c.unwrap().changed)
        .collect()
        .await;
    assert_eq!(changed, vec![false, false, true, true, true]);
    let presence: Vec<bool> = client
        .put_multi_get_previous_presence(0, column, futures::stream::iter((93..98u64).map(move |k| big(k, b'q'))))
        .await
        .unwrap()
        .map(|p| p.unwrap().present)
        .collect()
        .await;
    assert_eq!(presence, vec![true, true, false, false, false]);
    let merged: Vec<_> = client
        .merge_multi_get_merged(0, column, futures::stream::iter((100..105u64).map(move |k| big(k, b'm'))))
        .await
        .unwrap()
        .map(|m| m.unwrap().merged.map(|v| v.len()))
        .collect()
        .await;
    assert_eq!(merged, vec![Some(large); 5], "each merged once, into an empty key");

    // RA-2.6b: two copies of one numbered multi write with results (same request id), as after an unknown outcome:
    // the second is answered from the server's record with the first's results, and applies nothing.
    let multi_copy = |sequence: i64| {
        client.with_context(
            RequestContext::batch_with_timeout(Duration::from_secs(30)).with_request_id(session, sequence, 0),
        )
    };
    let merge_items = || {
        futures::stream::iter((110..113u64).map(|k| Kv {
            keys: key(k),
            value: format!("r{k}").into_bytes(),
        }))
    };
    let mut answers = Vec::new();
    for _ in 0..2 {
        let merged: Vec<_> = multi_copy(1)
            .merge_multi_get_merged(0, column, merge_items())
            .await
            .unwrap()
            .map(|m| text(&m.unwrap().merged))
            .collect()
            .await;
        answers.push(merged);
    }
    let expected: Vec<_> = (110..113u64).map(|k| Some(format!("r{k}"))).collect();
    assert_eq!(answers, vec![expected.clone(), expected], "the copy answers with the first's results");
    let put_items = || {
        futures::stream::iter((110..112u64).map(|k| Kv {
            keys: key(k),
            value: format!("s{k}").into_bytes(),
        }))
    };
    let mut answers = Vec::new();
    for _ in 0..2 {
        let previous: Vec<_> = multi_copy(2)
            .put_multi_get_previous(0, column, put_items())
            .await
            .unwrap()
            .map(|p| text(&p.unwrap().previous))
            .collect()
            .await;
        answers.push(previous);
    }
    let expected = vec![Some("r110".to_owned()), Some("r111".to_owned())];
    assert_eq!(answers, vec![expected.clone(), expected], "the copy answers with the first's previous values");
    for k in 110..113u64 {
        let expected = if k < 112 { format!("s{k}") } else { format!("r{k}") };
        assert_eq!(text(&client.get(0, column, key(k)).await.unwrap()), Some(expected), "key {k}");
    }

    // Unnumbered writes still work.
    let plain = client.without_client_sessions();
    assert!(!plain.client_sessions());
    plain.put(0, column, key(60), b"plain".to_vec()).await.unwrap();
    assert_eq!(text(&client.get(0, column, key(60)).await.unwrap()).as_deref(), Some("plain"));
}

/// RA-2.6b: numbered writes whose calls are lost with a real leader. `RustClientLiveTest` drives it through the files
/// of `ROCKSERVER_TEST_SIGNAL_DIR`: `ROCKSERVER_TEST_ADDR` is a proxy to the leader. Once this test wrote `warm`
/// (the session is open), the Java side stops the leader's apply and writes `go`; the writes below then commit on the
/// followers but get no answer. The Java side halts the leader, which loses the calls, and points the proxy at a
/// follower: the client sends the same numbered writes again, and the follower's forward to the new leader answers
/// them from the record, with the first copy's results, applied once.
#[tokio::test]
#[ignore = "driven by RustClientLiveTest, with ROCKSERVER_TEST_ADDR and ROCKSERVER_TEST_SIGNAL_DIR"]
async fn numbered_writes_resent_across_a_leader_loss() {
    let address = std::env::var("ROCKSERVER_TEST_ADDR")
        .expect("ROCKSERVER_TEST_ADDR names the server to test against");
    let signals = std::path::PathBuf::from(
        std::env::var("ROCKSERVER_TEST_SIGNAL_DIR").expect("ROCKSERVER_TEST_SIGNAL_DIR names the signal directory"),
    );
    let client = RockserverClient::connect(address, RequestContext::batch_with_timeout(Duration::from_secs(60)))
        .await
        .expect("connect");
    assert!(client.client_sessions(), "a replicated server offers client sessions");
    let column = client.create_column("rust-failover".to_owned(), schema()).await.unwrap();
    client.put(0, column, key(5), b"old5".to_vec()).await.unwrap();
    client.put(0, column, key(6), b"old6".to_vec()).await.unwrap();
    std::fs::write(signals.join("warm"), b"").unwrap();
    let go = signals.join("go");
    let waited = std::time::Instant::now();
    while !go.exists() {
        assert!(waited.elapsed() < Duration::from_secs(60), "no go signal");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let merge = client.merge_get_merged(0, column, key(1), b"x".to_vec());
    let merge_multi = async {
        client
            .merge_multi_get_merged(
                0,
                column,
                futures::stream::iter((2..5u64).map(|k| Kv {
                    keys: key(k),
                    value: format!("m{k}").into_bytes(),
                })),
            )
            .await?
            .map(|m| m.map(|m| text(&m.merged)))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
    };
    let put_multi = async {
        client
            .put_multi_get_previous(
                0,
                column,
                futures::stream::iter((5..7u64).map(|k| Kv {
                    keys: key(k),
                    value: format!("new{k}").into_bytes(),
                })),
            )
            .await?
            .map(|p| p.map(|p| text(&p.previous)))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
    };
    let (merged, multi_merged, previous) = tokio::join!(merge, merge_multi, put_multi);
    assert_eq!(text(&merged.unwrap()).as_deref(), Some("x"), "merged once");
    assert_eq!(
        multi_merged.unwrap(),
        vec![Some("m2".to_owned()), Some("m3".to_owned()), Some("m4".to_owned())],
        "the multi merge's results, merged once"
    );
    assert_eq!(
        previous.unwrap(),
        vec![Some("old5".to_owned()), Some("old6".to_owned())],
        "the first copy's previous values"
    );
    let resends = client.numbered_resends();
    println!("RESENDS {resends}");
    assert!(resends > 0, "the writes were lost with the leader and sent again");

    // A later write of the same session, on the new leader.
    assert_eq!(
        text(&client.merge_get_merged(0, column, key(1), b"y".to_vec()).await.unwrap()).as_deref(),
        Some("x,y")
    );
}
