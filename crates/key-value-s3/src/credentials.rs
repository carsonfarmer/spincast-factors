//! Renewable, per-manager credentials.
//!
//! In managed deployments each store manager holds its own short-lived,
//! prefix-scoped credentials (for example, STS session credentials minted by
//! the platform). Those expire while an application is parked, so the
//! manager needs a way to fetch fresh ones - without a process-global
//! credential chain, which cannot represent many independently scoped
//! applications in one process.
//!
//! The platform implements [`RenewCredentials`]; [`RefreshingCredentials`]
//! wraps it with caching, early renewal, and coalescing, and plugs into the
//! S3 client as its credential provider. Each manager gets its own instance:
//! nothing is shared between managers, and nothing is stored durably.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use object_store::CredentialProvider;
use object_store::aws::AwsCredential;

/// How long before expiry a cached credential is treated as stale, so
/// renewal happens before requests start failing.
const RENEWAL_MARGIN: Duration = Duration::from_secs(60);

/// One issuance of temporary credentials, scoped by the issuer.
#[derive(Clone)]
pub struct ScopedCredentials {
    /// The access key id.
    pub access_key: String,
    /// The secret access key.
    pub secret_key: String,
    /// The session token, for STS-issued credentials.
    pub token: Option<String>,
    /// When these credentials stop working. `None` means they do not
    /// expire.
    pub expires_at: Option<SystemTime>,
}

/// A source of fresh, scoped credentials.
///
/// Implemented by the platform host: typically an STS `AssumeRole` call
/// with a session policy pinned to this manager's prefix. Called on first
/// use and again shortly before (or after) each issuance expires. The
/// implementation must not persist anything durable.
#[async_trait]
pub trait RenewCredentials: Send + Sync + std::fmt::Debug {
    /// Returns a fresh credential issuance.
    async fn renew(&self) -> anyhow::Result<ScopedCredentials>;
}

/// Caches issuances from a [`RenewCredentials`] source and renews them
/// near expiry, coalescing concurrent renewals into one call.
#[derive(Debug)]
pub(crate) struct RefreshingCredentials {
    source: Arc<dyn RenewCredentials>,
    cached: tokio::sync::RwLock<Option<Cached>>,
    /// Held for the duration of one renewal, so concurrent callers wait for
    /// it instead of issuing their own.
    renewing: tokio::sync::Mutex<()>,
}

#[derive(Debug, Clone)]
struct Cached {
    credential: Arc<AwsCredential>,
    expires_at: Option<SystemTime>,
}

impl Cached {
    fn fresh(&self) -> bool {
        match self.expires_at {
            None => true,
            Some(at) => SystemTime::now() + RENEWAL_MARGIN < at,
        }
    }
}

impl RefreshingCredentials {
    pub(crate) fn new(source: Arc<dyn RenewCredentials>) -> Self {
        Self {
            source,
            cached: tokio::sync::RwLock::new(None),
            renewing: tokio::sync::Mutex::new(()),
        }
    }

    async fn cached_fresh(&self) -> Option<Arc<AwsCredential>> {
        let cached = self.cached.read().await;
        cached
            .as_ref()
            .filter(|c| c.fresh())
            .map(|c| c.credential.clone())
    }
}

#[async_trait]
impl CredentialProvider for RefreshingCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        if let Some(credential) = self.cached_fresh().await {
            return Ok(credential);
        }

        let _renewal = self.renewing.lock().await;
        // Whoever held the lock before us may have renewed already.
        if let Some(credential) = self.cached_fresh().await {
            return Ok(credential);
        }

        let issued =
            self.source
                .renew()
                .await
                .map_err(|err| object_store::Error::Unauthenticated {
                    path: String::new(),
                    source: err.into(),
                })?;
        let credential = Arc::new(AwsCredential {
            key_id: issued.access_key,
            secret_key: issued.secret_key,
            token: issued.token,
        });
        *self.cached.write().await = Some(Cached {
            credential: credential.clone(),
            expires_at: issued.expires_at,
        });
        Ok(credential)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A deterministic source: counts calls, issues numbered credentials
    /// with a configurable lifetime, and can be told to fail.
    #[derive(Debug)]
    struct Fake {
        calls: AtomicUsize,
        lifetime: std::sync::Mutex<Option<Duration>>,
        fail: std::sync::Mutex<bool>,
        delay: Option<Duration>,
    }

    impl Fake {
        fn new(lifetime: Option<Duration>) -> Arc<Self> {
            Arc::new(Self {
                calls: AtomicUsize::new(0),
                lifetime: std::sync::Mutex::new(lifetime),
                fail: std::sync::Mutex::new(false),
                delay: None,
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RenewCredentials for Fake {
        async fn renew(&self) -> anyhow::Result<ScopedCredentials> {
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            if *self.fail.lock().unwrap() {
                anyhow::bail!("issuer unavailable");
            }
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let lifetime = *self.lifetime.lock().unwrap();
            Ok(ScopedCredentials {
                access_key: format!("AKIA{n}"),
                secret_key: format!("secret{n}"),
                token: Some(format!("token{n}")),
                expires_at: lifetime.map(|l| SystemTime::now() + l),
            })
        }
    }

    #[tokio::test]
    async fn first_use_loads_then_caches() {
        let fake = Fake::new(Some(Duration::from_secs(3600)));
        let provider = RefreshingCredentials::new(fake.clone());

        let first = provider.get_credential().await.unwrap();
        let second = provider.get_credential().await.unwrap();
        assert_eq!(first.key_id, "AKIA1");
        assert!(Arc::ptr_eq(&first, &second), "second use hits the cache");
        assert_eq!(fake.calls(), 1);
    }

    /// A manager parked past its credentials' lifetime renews on the next
    /// use instead of failing with the stale issuance.
    #[tokio::test]
    async fn expired_credentials_renew_on_next_use() {
        // Lifetime below the renewal margin: stale immediately.
        let fake = Fake::new(Some(Duration::from_secs(1)));
        let provider = RefreshingCredentials::new(fake.clone());

        let first = provider.get_credential().await.unwrap();
        let second = provider.get_credential().await.unwrap();
        assert_eq!(first.key_id, "AKIA1");
        assert_eq!(second.key_id, "AKIA2", "stale issuance is replaced");
        assert_eq!(fake.calls(), 2);
    }

    #[tokio::test]
    async fn renewal_failure_surfaces_then_recovers() {
        let fake = Fake::new(Some(Duration::from_secs(1)));
        let provider = RefreshingCredentials::new(fake.clone());
        provider.get_credential().await.unwrap();

        *fake.fail.lock().unwrap() = true;
        assert!(provider.get_credential().await.is_err());

        *fake.fail.lock().unwrap() = false;
        let recovered = provider.get_credential().await.unwrap();
        assert_eq!(recovered.key_id, "AKIA2");
    }

    #[tokio::test]
    async fn concurrent_first_use_renews_once() {
        let fake = Arc::new(Fake {
            calls: AtomicUsize::new(0),
            lifetime: std::sync::Mutex::new(Some(Duration::from_secs(3600))),
            fail: std::sync::Mutex::new(false),
            delay: Some(Duration::from_millis(50)),
        });
        let provider = Arc::new(RefreshingCredentials::new(
            fake.clone() as Arc<dyn RenewCredentials>
        ));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let provider = provider.clone();
            tasks.push(tokio::spawn(async move { provider.get_credential().await }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(fake.calls(), 1, "renewals are coalesced");
    }

    #[tokio::test]
    async fn managers_do_not_share_credentials() {
        let fake_a = Fake::new(None);
        let fake_b = Fake::new(None);
        let a = RefreshingCredentials::new(fake_a.clone());
        let b = RefreshingCredentials::new(fake_b.clone());

        let cred_a = a.get_credential().await.unwrap();
        let cred_b = b.get_credential().await.unwrap();
        assert_eq!(fake_a.calls(), 1);
        assert_eq!(fake_b.calls(), 1);
        // Same numbering from independent issuers proves neither leaked
        // into the other's cache.
        assert_eq!(cred_a.key_id, "AKIA1");
        assert_eq!(cred_b.key_id, "AKIA1");
        assert!(!Arc::ptr_eq(&cred_a, &cred_b));
    }
}
