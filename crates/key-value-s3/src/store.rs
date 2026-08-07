//! The S3-backed store implementation.

use std::sync::{Arc, Mutex};

use futures::{StreamExt as _, TryStreamExt as _};
use object_store::path::Path as StorePath;
use object_store::{GetOptions, ObjectStore, PutMode, PutOptions, PutPayload, UpdateVersion};
use spin_core::async_trait;
use spin_factor_key_value::{
    Cas, Error, Store, StoreManager, SwapError, log_error, log_error_v3, v3,
};

/// How many conditional-write attempts an `increment` makes before giving
/// up. Contention on a single counter re-reads and retries; a bound keeps a
/// pathological hot key from spinning forever.
const INCREMENT_ATTEMPTS: usize = 32;

/// Concurrency for the batch operations.
const BATCH_CONCURRENCY: usize = 8;

/// A key-value store manager over a bucket prefix.
///
/// Each labelled store lives under `<prefix>/<label>/`, so one platform-
/// injected prefix (conventionally the tenant root) hosts every store an
/// app declares without collisions.
pub struct KeyValueS3 {
    summary: String,
    root: StorePath,
    client: ClientCell,
}

/// The deferred client initialization a [`KeyValueS3`] is constructed
/// over: the AWS configuration chain loads asynchronously, and tests hand
/// in a ready in-memory store the same way.
pub type ClientFuture = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Arc<dyn ObjectStore>, String>> + Send>,
>;
type ClientCell = async_once_cell::Lazy<Result<Arc<dyn ObjectStore>, String>, ClientFuture>;

impl KeyValueS3 {
    /// Creates a manager whose client is initialized by `client` on first
    /// use (the AWS configuration chain loads asynchronously).
    pub fn new(summary: String, root: StorePath, client: ClientFuture) -> Self {
        Self {
            summary,
            root,
            client: async_once_cell::Lazy::from_future(client),
        }
    }
}

#[async_trait]
impl StoreManager for KeyValueS3 {
    async fn get(&self, name: &str) -> Result<Arc<dyn Store>, Error> {
        let client = match self.client.get_unpin().await {
            Ok(client) => client.clone(),
            Err(message) => return Err(Error::Other(message.clone())),
        };
        Ok(Arc::new(S3Store {
            store: client,
            root: join_encoded(&self.root, &encode_key(name)),
        }))
    }

    fn is_defined(&self, _store_name: &str) -> bool {
        // Any label resolves: it becomes a prefix segment under the root.
        true
    }

    fn summary(&self, store_name: &str) -> Option<String> {
        Some(format!("{}/{store_name}", self.summary))
    }
}

struct S3Store {
    store: Arc<dyn ObjectStore>,
    /// The prefix all of this store's objects live under.
    root: StorePath,
}

impl S3Store {
    fn path(&self, key: &str) -> StorePath {
        join_encoded(&self.root, &encode_key(key))
    }

    /// The current value and version of `key`, `None` when absent.
    async fn fetch(&self, key: &str) -> Result<Option<(Vec<u8>, UpdateVersion)>, Error> {
        match self.store.get(&self.path(key)).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result.bytes().await.map_err(store_err)?;
                Ok(Some((bytes.to_vec(), version)))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(store_err(err)),
        }
    }
}

#[async_trait]
impl Store for S3Store {
    async fn get(&self, key: &str, max_result_bytes: usize) -> Result<Option<Vec<u8>>, Error> {
        let value = self.fetch(key).await?.map(|(bytes, _)| bytes);
        let byte_count =
            std::mem::size_of::<Option<Vec<u8>>>() + value.as_ref().map_or(0, Vec::len);
        if byte_count > max_result_bytes {
            return Err(Error::Other(format!(
                "query result exceeds limit of {max_result_bytes} bytes"
            )));
        }
        Ok(value)
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), Error> {
        self.store
            .put(&self.path(key), PutPayload::from(value.to_vec()))
            .await
            .map_err(store_err)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(store_err(err)),
        }
    }

    async fn exists(&self, key: &str) -> Result<bool, Error> {
        match self.store.head(&self.path(key)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(store_err(err)),
        }
    }

    async fn get_keys(&self, max_result_bytes: usize) -> Result<Vec<String>, Error> {
        let mut keys = Vec::new();
        let mut byte_count = std::mem::size_of::<Vec<String>>();
        let mut listing = self.store.list(Some(&self.root));
        while let Some(meta) = listing.try_next().await.map_err(store_err)? {
            let Some(key) = decode_entry(&self.root, &meta.location) else {
                continue;
            };
            byte_count += std::mem::size_of::<String>() + key.len();
            if byte_count > max_result_bytes {
                return Err(Error::Other(format!(
                    "query result exceeds limit of {max_result_bytes} bytes"
                )));
            }
            keys.push(key);
        }
        Ok(keys)
    }

    async fn get_keys_async(
        &self,
        max_result_bytes: usize,
    ) -> (
        tokio::sync::mpsc::Receiver<String>,
        tokio::sync::oneshot::Receiver<Result<(), v3::Error>>,
    ) {
        let (keys_tx, keys_rx) = tokio::sync::mpsc::channel(4);
        let (err_tx, err_rx) = tokio::sync::oneshot::channel();

        let store = self.store.clone();
        let root = self.root.clone();
        let the_work = async move {
            let mut listing = store.list(Some(&root));
            while let Some(meta) = listing.try_next().await.map_err(log_error_v3)? {
                let Some(key) = decode_entry(&root, &meta.location) else {
                    continue;
                };
                if key.len() > max_result_bytes {
                    return Err(v3::Error::Other(format!(
                        "key exceeds limit of {max_result_bytes} bytes"
                    )));
                }
                keys_tx.send(key).await.map_err(log_error_v3)?;
            }
            Ok(())
        };
        tokio::spawn(async move {
            let res = the_work.await;
            _ = err_tx.send(res);
        });

        (keys_rx, err_rx)
    }

    async fn get_many(
        &self,
        keys: Vec<String>,
        max_result_bytes: usize,
    ) -> Result<Vec<(String, Option<Vec<u8>>)>, Error> {
        let mut fetched = futures::stream::iter(keys)
            .map(|key| async move {
                let value = self.fetch(&key).await?.map(|(bytes, _)| bytes);
                Ok::<_, Error>((key, value))
            })
            .buffer_unordered(BATCH_CONCURRENCY);

        let mut results = Vec::new();
        let mut byte_count = 0;
        while let Some(entry) = fetched.next().await {
            let (key, value) = entry?;
            byte_count += std::mem::size_of::<(String, Option<Vec<u8>>)>()
                + key.len()
                + value.as_ref().map_or(0, Vec::len);
            if byte_count > max_result_bytes {
                return Err(Error::Other(format!(
                    "query result exceeds limit of {max_result_bytes} bytes"
                )));
            }
            results.push((key, value));
        }
        Ok(results)
    }

    async fn set_many(&self, key_values: Vec<(String, Vec<u8>)>) -> Result<(), Error> {
        futures::stream::iter(key_values)
            .map(|(key, value)| async move { self.set(&key, &value).await })
            .buffer_unordered(BATCH_CONCURRENCY)
            .try_collect()
            .await
    }

    async fn delete_many(&self, keys: Vec<String>) -> Result<(), Error> {
        futures::stream::iter(keys)
            .map(|key| async move { self.delete(&key).await })
            .buffer_unordered(BATCH_CONCURRENCY)
            .try_collect()
            .await
    }

    async fn increment(&self, key: String, delta: i64) -> Result<i64, Error> {
        // Read-modify-write with a conditional put: the write succeeds only
        // if the object is unchanged since the read (or still absent), so
        // concurrent increments retry rather than lose updates.
        for _ in 0..INCREMENT_ATTEMPTS {
            let current = self.fetch(&key).await?;
            let old_val = match &current {
                Some((bytes, _)) => Some(
                    core::str::from_utf8(bytes)
                        .map_err(log_error)?
                        .parse::<i64>()
                        .map_err(log_error)?,
                ),
                None => None,
            };
            let new_val = old_val.unwrap_or(0) + delta;

            let options = PutOptions {
                mode: match current {
                    Some((_, version)) => PutMode::Update(version),
                    None => PutMode::Create,
                },
                ..Default::default()
            };
            match self
                .store
                .put_opts(
                    &self.path(&key),
                    PutPayload::from(new_val.to_string().into_bytes()),
                    options,
                )
                .await
            {
                Ok(_) => return Ok(new_val),
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => continue,
                Err(err) => return Err(store_err(err)),
            }
        }
        Err(Error::Other(format!(
            "increment of {key} kept losing conditional writes after {INCREMENT_ATTEMPTS} attempts"
        )))
    }

    async fn new_compare_and_swap(
        &self,
        bucket_rep: u32,
        key: &str,
    ) -> Result<Arc<dyn Cas>, Error> {
        Ok(Arc::new(S3Cas {
            store: self.store.clone(),
            path: self.path(key),
            key: key.to_string(),
            bucket_rep,
            state: Mutex::new(CasState::Unknown),
        }))
    }
}

#[derive(Clone)]
enum CasState {
    /// `current` saw this version.
    Versioned(UpdateVersion),
    /// `current` saw no object; the swap expects to create it.
    Unset,
    /// `current` was never called; the swap is unconditional.
    Unknown,
}

struct S3Cas {
    store: Arc<dyn ObjectStore>,
    path: StorePath,
    key: String,
    bucket_rep: u32,
    state: Mutex<CasState>,
}

#[async_trait]
impl Cas for S3Cas {
    async fn current(&self, max_result_bytes: usize) -> Result<Option<Vec<u8>>, Error> {
        let options = GetOptions::default();
        let value = match self.store.get_opts(&self.path, options).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                *self.state.lock().unwrap() = CasState::Versioned(version);
                Some(result.bytes().await.map_err(store_err)?.to_vec())
            }
            Err(object_store::Error::NotFound { .. }) => {
                *self.state.lock().unwrap() = CasState::Unset;
                None
            }
            Err(err) => return Err(store_err(err)),
        };
        let byte_count =
            std::mem::size_of::<Option<Vec<u8>>>() + value.as_ref().map_or(0, Vec::len);
        if byte_count > max_result_bytes {
            return Err(Error::Other(format!(
                "query result exceeds limit of {max_result_bytes} bytes"
            )));
        }
        Ok(value)
    }

    async fn swap(&self, value: Vec<u8>) -> Result<(), SwapError> {
        let mode = match self.state.lock().unwrap().clone() {
            CasState::Versioned(version) => PutMode::Update(version),
            CasState::Unset => PutMode::Create,
            CasState::Unknown => PutMode::Overwrite,
        };
        let options = PutOptions {
            mode,
            ..Default::default()
        };
        match self
            .store
            .put_opts(&self.path, PutPayload::from(value), options)
            .await
        {
            Ok(_) => Ok(()),
            Err(
                err @ (object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. }),
            ) => Err(SwapError::CasFailed(format!("{err}"))),
            Err(err) => Err(SwapError::Other(format!("{err}"))),
        }
    }

    async fn bucket_rep(&self) -> u32 {
        self.bucket_rep
    }

    async fn key(&self) -> String {
        self.key.clone()
    }
}

/// Joins an already-encoded segment onto `root` verbatim.
///
/// `Path::child` would apply `object_store`'s own percent-encoding on top
/// of [`encode_key`]'s; parsing keeps the segment as written. The encoding
/// only emits `[A-Za-z0-9._%-]` and never a bare `.`/`..` segment, all of
/// which `Path::parse` accepts - covered by the whole-byte-range test.
fn join_encoded(root: &StorePath, encoded: &str) -> StorePath {
    let joined = if root.as_ref().is_empty() {
        encoded.to_string()
    } else {
        format!("{}/{}", root.as_ref(), encoded)
    };
    StorePath::parse(joined).expect("encoded segments are always valid object paths")
}

fn store_err(err: object_store::Error) -> Error {
    match &err {
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => Error::AccessDenied,
        _ => log_error(err),
    }
}

/// Encodes a guest key as one object-path segment: bytes outside
/// `[A-Za-z0-9._-]` become `%XX`, so any key round-trips and no key can
/// smuggle a `/` (or a `..` segment) into the object path.
fn encode_key(key: &str) -> String {
    if key.is_empty() {
        // A bare `%` is not produced for any other key (a literal percent
        // byte encodes as `%25`), so it can stand in for the empty key.
        return "%".to_string();
    }
    let mut out = String::with_capacity(key.len());
    for byte in key.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' => out.push(byte as char),
            // A lone `.` or `..` segment is not addressable in an object
            // path; escaping every leading dot sidesteps both.
            b'.' if !out.is_empty() => out.push('.'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// The inverse of [`encode_key`], `None` for entries this store did not
/// write (foreign objects under the prefix are skipped, not errors).
fn decode_key(encoded: &str) -> Option<String> {
    if encoded == "%" {
        return Some(String::new());
    }
    let mut bytes = Vec::with_capacity(encoded.len());
    let mut rest = encoded.bytes();
    while let Some(byte) = rest.next() {
        if byte == b'%' {
            let high = rest.next()?;
            let low = rest.next()?;
            let hex = [high, low];
            let hex = core::str::from_utf8(&hex).ok()?;
            bytes.push(u8::from_str_radix(hex, 16).ok()?);
        } else {
            bytes.push(byte);
        }
    }
    String::from_utf8(bytes).ok()
}

/// The guest key for a listed object directly under `root`, `None` for
/// anything else (deeper paths, undecodable names).
fn decode_entry(root: &StorePath, location: &StorePath) -> Option<String> {
    let suffix = location.as_ref().strip_prefix(root.as_ref())?;
    let encoded = suffix.strip_prefix('/')?;
    if encoded.is_empty() || encoded.contains('/') {
        return None;
    }
    decode_key(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_byte_encodes_to_a_valid_path_segment() {
        // The `expect` in `join_encoded` rests on this: any single byte -
        // and by composition any string - encodes to something
        // `Path::parse` accepts and that decodes back exactly.
        let root = StorePath::parse("r").unwrap();
        for byte in 0..=255u8 {
            let key = String::from_utf8_lossy(&[byte]).into_owned();
            let encoded = encode_key(&key);
            let path = join_encoded(&root, &encoded);
            assert_eq!(path.as_ref(), format!("r/{encoded}"));
            assert_eq!(decode_key(&encoded).as_deref(), Some(key.as_str()));
        }
    }

    #[test]
    fn keys_round_trip_through_encoding() {
        for key in [
            "plain",
            "with/slash",
            "with space",
            "..",
            ".",
            ".hidden",
            "per%cent",
            "unicode-\u{1F980}",
            "",
        ] {
            let encoded = encode_key(key);
            assert!(!encoded.contains('/'), "{key}: {encoded}");
            assert_ne!(encoded, ".");
            assert_ne!(encoded, "..");
            assert_eq!(decode_key(&encoded).as_deref(), Some(key), "{key}");
        }
    }
}
