//! Integration suite against a real S3-protocol server, normally MinIO
//! running locally.
//!
//! The default test suite (`tests/store.rs`) runs against
//! `object_store::memory::InMemory` - fast and offline, but it does not
//! exercise a network endpoint: HTTP conditional writes, list pagination,
//! and error mapping from real server responses. This suite runs the same
//! operations against an S3-compatible server over HTTP.
//!
//! It is gated on `SPINCAST_S3_TEST_BUCKET`: without it every case skips,
//! so plain `cargo test` stays offline. CI starts a MinIO container and
//! runs it on every push - no cloud resources, no cloud credentials, ever.
//!
//! # Running locally
//!
//! ```sh
//! docker run -d --rm -p 9000:9000 --name spincast-minio \
//!     -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
//!     minio/minio server /data
//! curl -X PUT http://127.0.0.1:9000/spincast-test \
//!     --aws-sigv4 "aws:amz:us-east-1:s3" --user minioadmin:minioadmin
//!
//! SPINCAST_S3_TEST_BUCKET=spincast-test \
//! SPINCAST_S3_TEST_ENDPOINT=http://127.0.0.1:9000 \
//! SPINCAST_S3_TEST_REGION=us-east-1 \
//! AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
//!     cargo test -p spin-key-value-s3 --test s3_server
//! ```
//!
//! Every case writes under a unique run prefix and deletes everything it
//! created during teardown, verifying no residue remains. One honesty
//! note: MinIO speaks the S3 protocol but is not AWS; behavior differences
//! between the two are out of this suite's scope.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as StorePath;
use spin_factor_key_value::{Store, StoreManager, SwapError};
use spin_key_value_s3::KeyValueS3;

const NO_LIMIT: usize = usize::MAX;

/// The caller-supplied bucket, or `None` to skip the whole suite.
fn bucket() -> Option<String> {
    std::env::var("SPINCAST_S3_TEST_BUCKET")
        .ok()
        .filter(|b| !b.is_empty())
}

/// A macro that turns each test into a skip when the bucket is unset, so
/// the suite is a no-op offline without marking anything failed.
macro_rules! s3_test {
    ($name:ident, |$fixture:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            let Some(bucket) = bucket() else {
                eprintln!(
                    "skipping {}: SPINCAST_S3_TEST_BUCKET unset",
                    stringify!($name)
                );
                return;
            };
            let $fixture = Fixture::new(&bucket, stringify!($name)).await;
            let result = async { $body }.await;
            // Clean up regardless of pass/fail, then surface the failure.
            $fixture.teardown().await;
            result.unwrap();
        }
    };
}

/// One test's isolated store, rooted at a unique run prefix under the
/// bucket, plus a raw client for teardown verification.
struct Fixture {
    raw: Arc<dyn ObjectStore>,
    root: StorePath,
    store: Arc<dyn Store>,
}

impl Fixture {
    async fn new(bucket: &str, name: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let prefix = format!("spincast-kv-test/{nanos}-{name}");

        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
        if let Ok(region) = std::env::var("SPINCAST_S3_TEST_REGION") {
            builder = builder.with_region(region);
        }
        if let Ok(endpoint) = std::env::var("SPINCAST_S3_TEST_ENDPOINT") {
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }
        let raw: Arc<dyn ObjectStore> =
            Arc::new(builder.build().expect("build s3 client from environment"));

        let root = StorePath::parse(&prefix).unwrap();
        let manager = {
            let raw = raw.clone();
            KeyValueS3::new(
                format!("s3 {bucket}/{prefix}"),
                root.clone(),
                Box::pin(async move { Ok(raw) }),
            )
        };
        let store = manager.get("default").await.unwrap();
        Self { raw, root, store }
    }

    /// Deletes every object created under the run prefix and asserts none
    /// remain. Runs even when the test body failed.
    async fn teardown(&self) {
        use futures::TryStreamExt as _;
        let locations: Vec<StorePath> = self
            .raw
            .list(Some(&self.root))
            .map_ok(|meta| meta.location)
            .try_collect()
            .await
            .expect("list run prefix for cleanup");
        for location in locations {
            let _ = self.raw.delete(&location).await;
        }
        let residue: Vec<_> = self
            .raw
            .list(Some(&self.root))
            .try_collect()
            .await
            .expect("re-list run prefix");
        assert!(
            residue.is_empty(),
            "run prefix {} still holds {} objects after cleanup",
            self.root,
            residue.len()
        );
    }
}

s3_test!(roundtrip_and_delete, |fx| {
    let store = &fx.store;
    anyhow::ensure!(store.get("k", NO_LIMIT).await? == None);
    store.set("k", b"value").await?;
    anyhow::ensure!(store.get("k", NO_LIMIT).await? == Some(b"value".to_vec()));
    anyhow::ensure!(store.exists("k").await?);
    store.delete("k").await?;
    anyhow::ensure!(store.get("k", NO_LIMIT).await? == None);
    Ok::<_, anyhow::Error>(())
});

s3_test!(awkward_keys_and_listing, |fx| {
    let store = &fx.store;
    let keys = ["with/slash", "with space", "..", "", "🦀"];
    for key in keys {
        store.set(key, key.as_bytes()).await?;
    }
    let mut listed = store.get_keys(NO_LIMIT).await?;
    listed.sort();
    let mut expected: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
    expected.sort();
    anyhow::ensure!(listed == expected, "listed {listed:?}");
    for key in keys {
        store.delete(key).await?;
    }
    Ok::<_, anyhow::Error>(())
});

s3_test!(paginated_listing, |fx| {
    let store = &fx.store;
    // Exceed S3's 1000-key list page, so pagination is genuinely exercised.
    let count = 1050;
    let kvs: Vec<(String, Vec<u8>)> = (0..count)
        .map(|i| (format!("k{i:05}"), b"v".to_vec()))
        .collect();
    store.set_many(kvs).await?;
    let listed = store.get_keys(NO_LIMIT).await?;
    anyhow::ensure!(listed.len() == count, "listed {} of {count}", listed.len());
    let keys: Vec<String> = (0..count).map(|i| format!("k{i:05}")).collect();
    store.delete_many(keys).await?;
    Ok::<_, anyhow::Error>(())
});

s3_test!(concurrent_increment_loses_nothing, |fx| {
    let store = fx.store.clone();
    let workers = 8;
    let per_worker = 25;
    let mut tasks = Vec::new();
    for _ in 0..workers {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..per_worker {
                store.increment("n".into(), 1).await.unwrap();
            }
        }));
    }
    for task in tasks {
        task.await?;
    }
    let final_value = store.get("n", NO_LIMIT).await?.expect("counter exists");
    let expected = (workers * per_worker).to_string();
    anyhow::ensure!(
        final_value == expected.as_bytes(),
        "counter is {:?}, expected {expected}: an accepted increment was lost",
        String::from_utf8_lossy(&final_value)
    );
    store.delete("n").await?;
    Ok::<_, anyhow::Error>(())
});

s3_test!(cas_conflict_is_visible, |fx| {
    let store = &fx.store;
    store.set("k", b"one").await?;
    let winner = store.new_compare_and_swap(1, "k").await?;
    let loser = store.new_compare_and_swap(1, "k").await?;
    winner.current(NO_LIMIT).await?;
    loser.current(NO_LIMIT).await?;
    winner.swap(b"two".to_vec()).await?;
    let conflict = loser.swap(b"three".to_vec()).await;
    anyhow::ensure!(
        matches!(conflict, Err(SwapError::CasFailed(_))),
        "expected CasFailed, got {conflict:?}"
    );
    anyhow::ensure!(store.get("k", NO_LIMIT).await? == Some(b"two".to_vec()));
    store.delete("k").await?;
    Ok::<_, anyhow::Error>(())
});

s3_test!(cas_create_race_is_visible, |fx| {
    let store = &fx.store;
    let cas = store.new_compare_and_swap(0, "fresh").await?;
    anyhow::ensure!(cas.current(NO_LIMIT).await? == None);
    // A racing create between `current` and `swap` must be detected.
    store.set("fresh", b"raced").await?;
    let conflict = cas.swap(b"mine".to_vec()).await;
    anyhow::ensure!(
        matches!(conflict, Err(SwapError::CasFailed(_))),
        "expected CasFailed on create race, got {conflict:?}"
    );
    store.delete("fresh").await?;
    Ok::<_, anyhow::Error>(())
});
