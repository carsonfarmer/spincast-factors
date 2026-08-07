//! An S3-backed key-value store for Spin: AWS's pool model over one bucket.
//!
//! Every value is one object at `<prefix>/<store-label>/<encoded-key>`. One
//! bucket pools any number of tenants under per-tenant prefixes; storage
//! costs S3 prices, idles at zero requests, and per-tenant usage falls out
//! of prefix-native tooling (Storage Lens, inventory, access logs).
//!
//! # Identity comes from the environment, not the config
//!
//! In multi-tenant deployments, apps (and their runtime config) are
//! untrusted, so this provider is **environment-first**: the platform
//! injects `SPIN_KV_S3_BUCKET`, `SPIN_KV_S3_PREFIX` (the tenant root), and
//! prefix-scoped credentials into each tenant's process, and a tenant's
//! runtime config says nothing but `type = "s3"` per store label. The label
//! becomes a path segment under the tenant root, so a tenant can organize
//! stores freely and reach nothing else. Config fields exist as overrides
//! for single-tenant/self-hosted use where operator and tenant are the same
//! party.
//!
//! Either way, the enforcement is the credential: scope it to the prefix
//! (for fleets, one shared role plus an STS inline session policy per
//! tenant) and S3 refuses anything outside it - including listing, which
//! stays under an `s3:prefix` condition. Config values only ever choose
//! *which door to walk up to*; the credential decides which doors open.
//!
//! # Semantics
//!
//! `get`/`set`/`delete`/`exists`/`get_keys` map directly onto object
//! operations. `increment` and compare-and-swap use S3 conditional writes
//! (ETag `If-Match` / if-none-match), so they are correct under
//! concurrency, but contended counters retry where DynamoDB would resolve
//! atomically - hot-counter workloads belong in a DynamoDB-backed store.
//! Values are capped only by S3's 5 TB object limit; keys are
//! percent-encoded into single path segments, so any string round-trips.

#![deny(missing_docs)]

use std::sync::Arc;

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::provider::{ProvideCredentials as _, SharedCredentialsProvider};
use object_store::aws::{AmazonS3Builder, AwsCredential};
use object_store::path::Path as StorePath;
use object_store::{CredentialProvider, ObjectStore};
use serde::Deserialize;
use spin_core::async_trait;
use spin_factor_key_value::runtime_config::spin::MakeKeyValueStore;

mod store;
pub use store::{ClientFuture, KeyValueS3};

/// The environment variables the platform injects; each is the fallback
/// for the config field of the same name.
pub mod env {
    /// Fallback for `bucket`.
    pub const BUCKET: &str = "SPIN_KV_S3_BUCKET";
    /// Fallback for `prefix` - conventionally the tenant root.
    pub const PREFIX: &str = "SPIN_KV_S3_PREFIX";
    /// Fallback for `endpoint`, for S3-compatible stores.
    pub const ENDPOINT: &str = "SPIN_KV_S3_ENDPOINT";
    /// Fallback for `allow_http` (`1`/`true`), for local development.
    pub const ALLOW_HTTP: &str = "SPIN_KV_S3_ALLOW_HTTP";
}

/// The `s3` key-value store type.
#[derive(Default)]
pub struct S3KeyValueStore {
    _priv: (),
}

impl S3KeyValueStore {
    /// Creates a new `S3KeyValueStore`.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Runtime configuration for the S3 key-value store.
///
/// Every field is optional: in platform deployments the environment
/// carries all of them and a store table is just `type = "s3"`. Explicit
/// fields override the environment for self-hosted use.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3KeyValueRuntimeConfig {
    /// The bucket holding the stores. Falls back to `SPIN_KV_S3_BUCKET`.
    #[serde(default)]
    pub bucket: Option<String>,
    /// The key prefix stores live under, conventionally the tenant root.
    /// Falls back to `SPIN_KV_S3_PREFIX`, then to the bucket root.
    #[serde(default)]
    pub prefix: Option<String>,
    /// The bucket's region. Falls back to what the AWS configuration
    /// chain resolves.
    #[serde(default)]
    pub region: Option<String>,
    /// A custom endpoint, for S3-compatible stores. Falls back to
    /// `SPIN_KV_S3_ENDPOINT`, then the chain (`AWS_ENDPOINT_URL`).
    #[serde(default)]
    pub endpoint: Option<String>,
    /// The access key; paired with `secret_key`. When either half is
    /// absent the AWS configuration chain is used instead.
    #[serde(default)]
    pub access_key: Option<String>,
    /// The secret key; paired with `access_key`.
    #[serde(default)]
    pub secret_key: Option<String>,
    /// The session token accompanying temporary credentials.
    #[serde(default)]
    pub token: Option<String>,
    /// Permit `http://` endpoints. Falls back to `SPIN_KV_S3_ALLOW_HTTP`.
    #[serde(default)]
    pub allow_http: Option<bool>,
}

impl MakeKeyValueStore for S3KeyValueStore {
    const RUNTIME_CONFIG_TYPE: &'static str = "s3";

    type RuntimeConfig = S3KeyValueRuntimeConfig;

    type StoreManager = KeyValueS3;

    fn make_store(
        &self,
        runtime_config: Self::RuntimeConfig,
    ) -> anyhow::Result<Self::StoreManager> {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());

        let Some(bucket) = runtime_config.bucket.clone().or_else(|| var(env::BUCKET)) else {
            anyhow::bail!(
                "s3 key-value store requires a bucket, from the `bucket` field or {}",
                env::BUCKET
            );
        };
        let prefix = runtime_config
            .prefix
            .clone()
            .or_else(|| var(env::PREFIX))
            .unwrap_or_default();
        let endpoint = runtime_config
            .endpoint
            .clone()
            .or_else(|| var(env::ENDPOINT));
        let allow_http = runtime_config.allow_http.unwrap_or_else(|| {
            var(env::ALLOW_HTTP).is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        });

        let root = StorePath::parse(&prefix)
            .map_err(|err| anyhow::anyhow!("invalid s3 key-value prefix {prefix:?}: {err}"))?;
        let summary = if prefix.is_empty() {
            format!("s3 {bucket}")
        } else {
            format!("s3 {bucket}/{prefix}")
        };

        let client = Box::pin(build_client(BuildClient {
            bucket,
            region: runtime_config.region,
            endpoint,
            access_key: runtime_config.access_key,
            secret_key: runtime_config.secret_key,
            token: runtime_config.token,
            allow_http,
        }));
        Ok(KeyValueS3::new(summary, root, client))
    }
}

struct BuildClient {
    bucket: String,
    region: Option<String>,
    endpoint: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    token: Option<String>,
    allow_http: bool,
}

/// Builds the S3 client, resolving credentials the way Spin's other AWS
/// providers do: an explicit `access_key`/`secret_key` pair is used
/// directly, and otherwise the standard AWS configuration chain supplies
/// credentials with its own caching and refresh.
async fn build_client(config: BuildClient) -> Result<Arc<dyn ObjectStore>, String> {
    let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

    let mut region = config.region;
    let mut endpoint = config.endpoint;
    match (&config.access_key, &config.secret_key) {
        (Some(access_key), Some(secret_key)) => {
            builder = builder
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key);
            if let Some(token) = &config.token {
                builder = builder.with_token(token);
            }
        }
        _ => {
            let mut loader = aws_config::defaults(BehaviorVersion::latest());
            if let Some(region) = region.clone() {
                loader = loader.region(Region::new(region));
            }
            let sdk_config = loader.load().await;
            region = region.or_else(|| sdk_config.region().map(|r| r.to_string()));
            endpoint = endpoint.or_else(|| sdk_config.endpoint_url().map(str::to_string));
            let Some(provider) = sdk_config.credentials_provider() else {
                return Err(format!(
                    "s3 key-value store {}: no credentials in the AWS configuration chain",
                    config.bucket
                ));
            };
            builder = builder.with_credentials(Arc::new(ChainCredentials { provider }));
        }
    }

    if let Some(region) = region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if config.allow_http {
        builder = builder.with_allow_http(true);
    }

    match builder.build() {
        Ok(store) => Ok(Arc::new(store)),
        Err(err) => {
            let message = format!("invalid s3 key-value configuration: {err}");
            tracing::error!("{message}");
            Err(message)
        }
    }
}

/// Bridges the AWS SDK's credential chain into `object_store`'s provider
/// interface. Caching and refresh live in the chain, not here.
#[derive(Debug)]
struct ChainCredentials {
    provider: SharedCredentialsProvider,
}

#[async_trait]
impl CredentialProvider for ChainCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let credentials = self.provider.provide_credentials().await.map_err(|err| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(err),
            }
        })?;
        Ok(Arc::new(AwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(str::to_string),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_table_is_valid_config() {
        // The platform-deployment shape: everything from the environment.
        let config: S3KeyValueRuntimeConfig = toml::from_str("").unwrap();
        assert!(config.bucket.is_none());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let err = toml::from_str::<S3KeyValueRuntimeConfig>("namespace = \"x\"").unwrap_err();
        assert!(err.to_string().contains("namespace"));
    }

    #[test]
    fn bucket_is_required_from_somewhere() {
        // No table value and (in this test environment) no env value.
        let config: S3KeyValueRuntimeConfig = toml::from_str("").unwrap();
        if std::env::var(env::BUCKET).is_err() {
            assert!(S3KeyValueStore::new().make_store(config).is_err());
        }
    }
}
