# spincast-factors

Spin factor providers for multi-tenant platforms. Each crate plugs into a
seam Spin already ships — no fork, no patches; a runtime binary registers
the providers it wants to expose, one line each.

| crate | plugs into | what it is |
|---|---|---|
| [`spin-key-value-s3`](crates/key-value-s3) | `spin-factor-key-value` | Pooled S3 key-value store: one bucket, prefix per tenant, `s3:prefix`-scoped credentials |

## The tenant model

A tenant's runtime config carries only *intent* — which labelled store to
use:

```toml
# The whole tenant-side config:
[key_value_store.default]
type = "s3"
```

The label becomes a path segment under the tenant's root, so tenants
organize stores freely and can address nothing else. Identity — bucket,
tenant-root prefix, credentials — comes from whoever is trusted, in one of
two construction modes:

- **Managed** (`ManagedStore` + `ManagedS3KeyValueStore`): for shared
  multi-tenant runtimes. The platform host supplies identity per store
  manager from its own records. The tenant-facing table is empty by
  definition — any field a tenant writes is rejected as an unknown field
  before a client exists. No environment variables are read: process-global
  state cannot represent many tenants in one process. Credentials are
  typically renewable STS issuances (`RenewCredentials`) scoped to the
  tenant prefix; the provider caches each issuance, renews it near expiry,
  and coalesces concurrent renewals — so a parked app resumes cleanly after
  its credentials expired.
- **Self-hosted / BYO** (`S3KeyValueStore`): operator and tenant are the
  same party. The table carries identity, with `SPIN_KV_S3_*` environment
  fallbacks for a one-tenant-per-process shape.

The modes never mix: both makers claim the `s3` type string, so
registering both in one runtime fails loudly at startup.

Either way the enforcement is the credential: the platform mints each
tenant's credentials (one shared IAM role + an STS inline session policy
scoping to the tenant prefix) outside tenant reach, and S3 itself refuses
anything beyond them. Config chooses which door to walk up to; the
credential decides which doors open.

(S3 Express One Zone is deliberately not supported: directory buckets
authorize at bucket granularity, so prefix-scoped credentials - the pool
model's isolation - cannot apply. Revisit if AWS adds prefix-scoped
authorization there.)

## Registering in a runtime

Spin composes factors and providers at compile time. A platform runtime
(or a fork's `runtime-config` crate) registers providers explicitly:

```rust
key_value_resolver
    .register_store_type(spin_key_value_s3::S3KeyValueStore::new())
    .unwrap();
```

Registering *only* credential-walled providers is the platform's second
wall: store types whose config values are their own boundary simply do not
exist in a tenant-facing runtime.

## Developing

```sh
cargo test --workspace    # includes behavioural tests over an in-memory
                          # object store - the same code path as a bucket
cargo clippy --workspace --all-targets
```

Spin is a git dependency on **upstream** `spinframework/spin`, pinned
to the latest release (`v4.0.2`); the crates use only interfaces stock
Spin already ships. Nothing here requires the Spincast fork. The full
test suite also passes against the tip of upstream `main`
(`58b0f6d8`, 2026-08).

## License

Apache-2.0 WITH LLVM-exception, matching Spin.
