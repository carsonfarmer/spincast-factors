//! Behavioural tests over `object_store`'s in-memory implementation - the
//! same code path a bucket takes, minus the network. Conditional writes
//! (the machinery under `increment` and compare-and-swap) behave like S3's.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use spin_factor_key_value::{Store, StoreManager, SwapError};
use spin_key_value_s3::KeyValueS3;

const NO_LIMIT: usize = usize::MAX;

fn manager_at(store: Arc<dyn ObjectStore>, prefix: &str) -> KeyValueS3 {
    KeyValueS3::new(
        format!("test {prefix}"),
        StorePath::parse(prefix).unwrap(),
        Box::pin(async move { Ok(store) }),
    )
}

async fn store_at(shared: &Arc<dyn ObjectStore>, prefix: &str, label: &str) -> Arc<dyn Store> {
    manager_at(shared.clone(), prefix).get(label).await.unwrap()
}

fn shared() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

#[tokio::test]
async fn roundtrip_exists_delete() {
    let store = store_at(&shared(), "tenant-a", "default").await;

    assert_eq!(store.get("k", NO_LIMIT).await.unwrap(), None);
    assert!(!store.exists("k").await.unwrap());

    store.set("k", b"value").await.unwrap();
    assert_eq!(
        store.get("k", NO_LIMIT).await.unwrap(),
        Some(b"value".to_vec())
    );
    assert!(store.exists("k").await.unwrap());

    // Overwrite in place.
    store.set("k", b"other").await.unwrap();
    assert_eq!(
        store.get("k", NO_LIMIT).await.unwrap(),
        Some(b"other".to_vec())
    );

    // Delete is idempotent.
    store.delete("k").await.unwrap();
    store.delete("k").await.unwrap();
    assert_eq!(store.get("k", NO_LIMIT).await.unwrap(), None);
}

#[tokio::test]
async fn awkward_keys_round_trip() {
    let store = store_at(&shared(), "tenant-a", "default").await;
    for key in ["with/slash", "with space", ".", "..", "", "per%cent", "🦀"] {
        store.set(key, key.as_bytes()).await.unwrap();
        assert_eq!(
            store.get(key, NO_LIMIT).await.unwrap().as_deref(),
            Some(key.as_bytes()),
            "{key:?}"
        );
    }
    let mut keys = store.get_keys(NO_LIMIT).await.unwrap();
    keys.sort();
    let mut expected = vec!["with/slash", "with space", ".", "..", "", "per%cent", "🦀"];
    expected.sort_unstable();
    assert_eq!(keys, expected);
}

#[tokio::test]
async fn labels_and_prefixes_isolate() {
    let shared = shared();
    let a_default = store_at(&shared, "tenant-a", "default").await;
    let a_cache = store_at(&shared, "tenant-a", "cache").await;
    let b_default = store_at(&shared, "tenant-b", "default").await;

    a_default.set("k", b"a-default").await.unwrap();
    a_cache.set("k", b"a-cache").await.unwrap();

    // Same key, different label or prefix: distinct values, distinct lists.
    assert_eq!(
        a_default.get("k", NO_LIMIT).await.unwrap().as_deref(),
        Some(&b"a-default"[..])
    );
    assert_eq!(
        a_cache.get("k", NO_LIMIT).await.unwrap().as_deref(),
        Some(&b"a-cache"[..])
    );
    assert_eq!(b_default.get("k", NO_LIMIT).await.unwrap(), None);
    assert!(b_default.get_keys(NO_LIMIT).await.unwrap().is_empty());
    assert_eq!(a_default.get_keys(NO_LIMIT).await.unwrap(), ["k"]);
}

#[tokio::test]
async fn foreign_objects_under_the_prefix_are_not_keys() {
    let shared = shared();
    let store = store_at(&shared, "tenant-a", "default").await;
    store.set("mine", b"x").await.unwrap();

    // Something else wrote deeper paths under the store's prefix.
    shared
        .put(
            &StorePath::parse("tenant-a/default/nested/deep").unwrap(),
            object_store::PutPayload::from_static(b"foreign"),
        )
        .await
        .unwrap();

    assert_eq!(store.get_keys(NO_LIMIT).await.unwrap(), ["mine"]);
}

#[tokio::test]
async fn batch_operations() {
    let store = store_at(&shared(), "t", "default").await;
    store
        .set_many(vec![
            ("a".into(), b"1".to_vec()),
            ("b".into(), b"2".to_vec()),
            ("c".into(), b"3".to_vec()),
        ])
        .await
        .unwrap();

    let mut many = store
        .get_many(vec!["a".into(), "b".into(), "missing".into()], NO_LIMIT)
        .await
        .unwrap();
    many.sort();
    assert_eq!(
        many,
        vec![
            ("a".to_string(), Some(b"1".to_vec())),
            ("b".to_string(), Some(b"2".to_vec())),
            ("missing".to_string(), None),
        ]
    );

    store
        .delete_many(vec!["a".into(), "b".into(), "missing".into()])
        .await
        .unwrap();
    assert_eq!(store.get_keys(NO_LIMIT).await.unwrap(), ["c"]);
}

#[tokio::test]
async fn increment_is_atomic_under_concurrency() {
    let store = store_at(&shared(), "t", "default").await;
    assert_eq!(store.increment("n".into(), 5).await.unwrap(), 5);
    assert_eq!(store.increment("n".into(), -2).await.unwrap(), 3);

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..10 {
                store.increment("n".into(), 1).await.unwrap();
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        store.get("n", NO_LIMIT).await.unwrap().as_deref(),
        Some(&b"83"[..])
    );
}

#[tokio::test]
async fn compare_and_swap_detects_conflicts() {
    let store = store_at(&shared(), "t", "default").await;
    store.set("k", b"one").await.unwrap();

    let winner = store.new_compare_and_swap(7, "k").await.unwrap();
    let loser = store.new_compare_and_swap(7, "k").await.unwrap();
    assert_eq!(
        winner.current(NO_LIMIT).await.unwrap(),
        Some(b"one".to_vec())
    );
    assert_eq!(
        loser.current(NO_LIMIT).await.unwrap(),
        Some(b"one".to_vec())
    );
    assert_eq!(winner.bucket_rep().await, 7);
    assert_eq!(winner.key().await, "k");

    winner.swap(b"two".to_vec()).await.unwrap();
    assert!(matches!(
        loser.swap(b"three".to_vec()).await,
        Err(SwapError::CasFailed(_))
    ));
    assert_eq!(
        store.get("k", NO_LIMIT).await.unwrap().as_deref(),
        Some(&b"two"[..])
    );
}

#[tokio::test]
async fn compare_and_swap_on_absent_key_expects_creation() {
    let store = store_at(&shared(), "t", "default").await;

    let cas = store.new_compare_and_swap(0, "fresh").await.unwrap();
    assert_eq!(cas.current(NO_LIMIT).await.unwrap(), None);

    // Someone else creates the key between `current` and `swap`.
    store.set("fresh", b"raced").await.unwrap();
    assert!(matches!(
        cas.swap(b"mine".to_vec()).await,
        Err(SwapError::CasFailed(_))
    ));

    // Without the race, creation succeeds.
    let cas = store.new_compare_and_swap(0, "fresh2").await.unwrap();
    assert_eq!(cas.current(NO_LIMIT).await.unwrap(), None);
    cas.swap(b"mine".to_vec()).await.unwrap();
    assert_eq!(
        store.get("fresh2", NO_LIMIT).await.unwrap().as_deref(),
        Some(&b"mine"[..])
    );
}

#[tokio::test]
async fn result_size_limits_are_enforced() {
    let store = store_at(&shared(), "t", "default").await;
    store.set("big", &[0u8; 1024]).await.unwrap();

    assert!(store.get("big", 64).await.is_err());
    assert!(store.get("big", NO_LIMIT).await.is_ok());

    for i in 0..64 {
        store.set(&format!("key-{i}"), b"x").await.unwrap();
    }
    assert!(store.get_keys(64).await.is_err());
    assert!(store.get_keys(NO_LIMIT).await.is_ok());
}

// --- Operation and memory bounds (issue #3) ---

use spin_key_value_s3::Limits;

/// A store with tiny, exact limits so bound tests read clearly.
async fn bounded_store(limits: Limits) -> Arc<dyn Store> {
    KeyValueS3::new(
        "test".into(),
        StorePath::parse("t").unwrap(),
        Box::pin(async move { Ok(shared()) }),
    )
    .with_limits(limits)
    .get("default")
    .await
    .unwrap()
}

#[tokio::test]
async fn value_size_bound_is_exact() {
    let limits = Limits {
        max_value_bytes: 100,
        ..Default::default()
    };
    let store = bounded_store(limits).await;

    store.set("k", &[0u8; 100]).await.unwrap(); // exactly at limit: ok
    let over = store.set("k", &[0u8; 101]).await; // one over: rejected
    assert!(over.is_err());
    // The error carries only sizes, never the value bytes.
    let msg = format!("{:?}", over.unwrap_err());
    assert!(msg.contains("101") && msg.contains("100"), "{msg}");
}

#[tokio::test]
async fn key_size_bound_is_exact() {
    let limits = Limits {
        max_key_bytes: 8,
        ..Default::default()
    };
    let store = bounded_store(limits).await;

    store.set("12345678", b"v").await.unwrap(); // encoded length 8: ok
    assert!(store.set("123456789", b"v").await.is_err()); // 9: rejected
    // Reads of an over-limit key are refused before any network call.
    assert!(store.get("123456789", NO_LIMIT).await.is_err());
    assert!(store.exists("123456789").await.is_err());
    assert!(store.delete("123456789").await.is_err());
}

#[tokio::test]
async fn batch_length_bound_is_exact() {
    let limits = Limits {
        max_batch_len: 3,
        ..Default::default()
    };
    let store = bounded_store(limits).await;

    let three: Vec<(String, Vec<u8>)> = (0..3).map(|i| (format!("k{i}"), b"v".to_vec())).collect();
    store.set_many(three).await.unwrap(); // exactly at limit: ok

    let four: Vec<(String, Vec<u8>)> = (0..4).map(|i| (format!("k{i}"), b"v".to_vec())).collect();
    assert!(store.set_many(four).await.is_err()); // one over: rejected

    let four_keys: Vec<String> = (0..4).map(|i| format!("k{i}")).collect();
    assert!(store.get_many(four_keys.clone(), NO_LIMIT).await.is_err());
    assert!(store.delete_many(four_keys).await.is_err());
}

#[tokio::test]
async fn batch_aggregate_bytes_bound() {
    let limits = Limits {
        max_value_bytes: 1000,
        max_batch_bytes: 150,
        ..Default::default()
    };
    let store = bounded_store(limits).await;

    // Each value is within max_value_bytes, but together they exceed the
    // batch aggregate.
    let kvs: Vec<(String, Vec<u8>)> = (0..3).map(|i| (format!("k{i}"), vec![0u8; 100])).collect();
    assert!(store.set_many(kvs).await.is_err());
}

#[tokio::test]
async fn hostile_batch_is_rejected_before_any_write() {
    let limits = Limits {
        max_batch_len: 10,
        ..Default::default()
    };
    let store = bounded_store(limits).await;

    // A million-entry batch never touches the store.
    let huge: Vec<(String, Vec<u8>)> = (0..1_000_000)
        .map(|i| (format!("k{i}"), b"v".to_vec()))
        .collect();
    assert!(store.set_many(huge).await.is_err());
    assert!(store.get_keys(NO_LIMIT).await.unwrap().is_empty());
}

#[tokio::test]
async fn increment_attempts_are_bounded() {
    // One attempt: the loop cannot spin. A normal increment still succeeds.
    let limits = Limits {
        max_conditional_attempts: 1,
        ..Default::default()
    };
    let store = bounded_store(limits).await;
    assert_eq!(store.increment("n".into(), 3).await.unwrap(), 3);
}
