//! Managed construction: the trust boundary for shared multi-tenant
//! runtimes.
//!
//! The default [`S3KeyValueStore`](crate::S3KeyValueStore) maker is for
//! self-hosted use: the operator and the tenant are the same party, so
//! letting the config table (or process environment) choose the bucket and
//! credentials is fine. In a managed platform neither is acceptable as a
//! trust boundary: tenant-authored tables must not redirect requests, and
//! process-global environment variables cannot represent many tenants in
//! one process.
//!
//! In managed mode the platform host constructs a [`ManagedStore`] from its
//! own attachment data - bucket, root prefix, endpoint, and a credential
//! source, typically [renewable](crate::RenewCredentials) STS session
//! credentials scoped to the prefix - and turns it into the store manager.
//! The tenant-facing config table for the type is **empty by definition**:
//! any field a tenant writes (`bucket`, `prefix`, `credentials`, anything)
//! is an unknown field and fails deserialization before a client exists or
//! a request is made. There is nothing to override because nothing is read.
//!
//! The two modes never mix. A managed manager reads no environment
//! variables and no table fields; the self-hosted maker never falls back to
//! platform credentials. Registering both makers for the same type string
//! fails loudly at startup, so a runtime is one or the other.

use std::sync::Arc;

use object_store::aws::{AmazonS3Builder, AwsCredential};
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, StaticCredentialProvider};
use serde::Deserialize;
use spin_factor_key_value::runtime_config::spin::MakeKeyValueStore;

use crate::credentials::{RefreshingCredentials, RenewCredentials};
use crate::store::KeyValueS3;

/// Trusted, host-supplied identity for one store manager.
///
/// Every field comes from the platform's own records, never from tenant
/// config or the process environment.
pub struct ManagedStore {
    /// The bucket holding the tenant's stores.
    pub bucket: String,
    /// The tenant's root prefix. Store labels nest beneath it.
    pub prefix: String,
    /// The bucket's region.
    pub region: Option<String>,
    /// A custom endpoint, for S3-compatible stores.
    pub endpoint: Option<String>,
    /// Permit `http://` endpoints (local development platforms only).
    pub allow_http: bool,
    /// The credential source, scoped to `prefix`.
    pub credentials: ManagedCredentials,
}

/// The credential source for a [`ManagedStore`].
pub enum ManagedCredentials {
    /// A fixed issuance, for processes whose lifetime is comfortably
    /// shorter than the credentials' (scale-to-zero shapes).
    Static {
        /// The access key id.
        access_key: String,
        /// The secret access key.
        secret_key: String,
        /// The session token, for STS-issued credentials.
        token: Option<String>,
    },
    /// A renewable source, for resident or parkable processes: called on
    /// first use and again near each issuance's expiry.
    Renewable(Arc<dyn RenewCredentials>),
}

impl ManagedStore {
    /// Builds the store manager.
    ///
    /// Fails only on malformed identity (an invalid prefix or bucket
    /// configuration); no network access happens here or later during
    /// construction - the client is built lazily on first use.
    pub fn into_manager(self) -> anyhow::Result<KeyValueS3> {
        let root = StorePath::parse(&self.prefix)
            .map_err(|err| anyhow::anyhow!("invalid managed prefix {:?}: {err}", self.prefix))?;
        let summary = if self.prefix.is_empty() {
            format!("s3 {}", self.bucket)
        } else {
            format!("s3 {}/{}", self.bucket, self.prefix)
        };

        let mut builder = AmazonS3Builder::new().with_bucket_name(&self.bucket);
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }
        builder = builder.with_credentials(match self.credentials {
            ManagedCredentials::Static {
                access_key,
                secret_key,
                token,
            } => Arc::new(StaticCredentialProvider::new(AwsCredential {
                key_id: access_key,
                secret_key,
                token,
            })),
            ManagedCredentials::Renewable(source) => Arc::new(RefreshingCredentials::new(source)),
        });
        let store: Arc<dyn ObjectStore> = Arc::new(
            builder
                .build()
                .map_err(|err| anyhow::anyhow!("invalid managed s3 configuration: {err}"))?,
        );

        Ok(KeyValueS3::new(
            summary,
            root,
            Box::pin(async move { Ok(store) }),
        ))
    }
}

/// The maker for managed runtimes: tenant tables select the type and
/// nothing else.
///
/// Construct it with a factory that produces the [`ManagedStore`] for the
/// application being served, from the platform's own attachment data.
pub struct ManagedS3KeyValueStore {
    make: Box<dyn Fn() -> anyhow::Result<ManagedStore> + Send + Sync>,
}

impl ManagedS3KeyValueStore {
    /// Creates a maker that builds each manager from `make`.
    pub fn new(make: impl Fn() -> anyhow::Result<ManagedStore> + Send + Sync + 'static) -> Self {
        Self {
            make: Box::new(make),
        }
    }
}

/// The tenant-facing configuration for a managed `s3` store: deliberately
/// empty. Any field present is unknown and rejects the config before a
/// client exists.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedS3KeyValueRuntimeConfig {}

impl MakeKeyValueStore for ManagedS3KeyValueStore {
    const RUNTIME_CONFIG_TYPE: &'static str = "s3";

    type RuntimeConfig = ManagedS3KeyValueRuntimeConfig;

    type StoreManager = KeyValueS3;

    fn make_store(
        &self,
        ManagedS3KeyValueRuntimeConfig {}: Self::RuntimeConfig,
    ) -> anyhow::Result<Self::StoreManager> {
        (self.make)()?.into_manager()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spin_factor_key_value::StoreManager as _;

    fn managed(prefix: &str) -> ManagedStore {
        ManagedStore {
            bucket: "platform-bucket".into(),
            prefix: prefix.into(),
            region: Some("us-east-1".into()),
            endpoint: None,
            allow_http: false,
            credentials: ManagedCredentials::Static {
                access_key: "AKIA".into(),
                secret_key: "secret".into(),
                token: Some("token".into()),
            },
        }
    }

    /// The core of the boundary: a tenant table can select the type and
    /// nothing else. Every identity field is rejected as unknown.
    #[test]
    fn tenant_tables_cannot_carry_identity() {
        assert!(toml::from_str::<ManagedS3KeyValueRuntimeConfig>("").is_ok());
        for field in [
            "bucket = \"other\"",
            "prefix = \"tenant-b\"",
            "region = \"eu-west-1\"",
            "endpoint = \"http://attacker.example\"",
            "access_key = \"AKIA\"",
            "secret_key = \"s\"",
            "token = \"t\"",
            "allow_http = true",
        ] {
            let err = toml::from_str::<ManagedS3KeyValueRuntimeConfig>(field)
                .expect_err(&format!("{field} must be rejected"));
            assert!(err.to_string().contains("unknown field"), "{field}: {err}");
        }
    }

    #[test]
    fn maker_builds_from_host_input_only() {
        let maker = ManagedS3KeyValueStore::new(|| Ok(managed("tenant-a")));
        let manager = maker.make_store(ManagedS3KeyValueRuntimeConfig {}).unwrap();
        assert_eq!(
            manager.summary("default").unwrap(),
            "s3 platform-bucket/tenant-a/default"
        );
    }

    /// Two managers in one process, no shared identity: distinct roots.
    #[test]
    fn managers_have_disjoint_roots() {
        let a = managed("tenants/a").into_manager().unwrap();
        let b = managed("tenants/b").into_manager().unwrap();
        let a_root = a.summary("default").unwrap();
        let b_root = b.summary("default").unwrap();
        assert_ne!(a_root, b_root);
        assert!(a_root.ends_with("tenants/a/default"));
        assert!(b_root.ends_with("tenants/b/default"));
    }

    #[test]
    fn malformed_identity_fails_closed() {
        let mut store = managed("ok");
        store.prefix = "not//valid".into();
        assert!(store.into_manager().is_err());
    }
}
