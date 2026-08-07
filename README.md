# spincast-factors

Spin factor providers for multi-tenant platforms. Each crate plugs into a
seam Spin already ships — no fork, no patches; a runtime binary registers
the providers it wants to expose, one line each.

| crate | plugs into | what it is |
|---|---|---|
| [`spin-key-value-s3`](crates/key-value-s3) | `spin-factor-key-value` | Pooled S3 key-value store: one bucket, prefix per tenant, `s3:prefix`-scoped credentials |

## The tenant model

These providers are **environment-first**: platform-injected environment
variables carry *identity* (bucket, tenant-root prefix, scoped
credentials), and a tenant's runtime config carries only *intent* — which
labelled store to use:

```toml
# The whole tenant-side config:
[key_value_store.default]
type = "s3"
```

The label becomes a path segment under the tenant's root, so tenants
organize stores freely and can address nothing else. Because tenants may
author their own runtime config, no config value is trusted for isolation:
the platform mints each tenant's credentials (one shared IAM role + an STS
inline session policy scoping to the tenant prefix) outside tenant reach,
and the store service itself refuses anything beyond them. Config chooses
which door to walk up to; the credential decides which doors open.

Config fields (`bucket`, `prefix`, `region`, `endpoint`, credentials,
`allow_http`, …) exist as overrides for single-tenant/self-hosted use,
where operator and tenant are the same party.

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

### Real-S3 qualification

The default suite runs against an in-memory object store — fast, offline,
and thorough for the adapter, but not proof of AWS S3's error mapping,
pagination, read-after-write visibility, or credential handling. A separate
opt-in suite runs the same operations against a real, disposable bucket:

```sh
SPINCAST_S3_TEST_BUCKET=my-disposable-bucket SPINCAST_S3_TEST_REGION=us-east-1     cargo test -p spin-key-value-s3 --test real_s3 -- --test-threads=1
```

Credentials come from the standard AWS chain. Without `SPINCAST_S3_TEST_BUCKET`
every case skips, so ordinary `cargo test` and CI never create live
resources. Each case writes under a unique run prefix and deletes everything
it created during teardown, verifying no residue remains.

Spin is a git dependency on **upstream** `spinframework/spin`, pinned
to the latest release (`v4.0.2`); the crates use only interfaces stock
Spin already ships. Nothing here requires the Spincast fork. The full
test suite also passes against the tip of upstream `main`
(`58b0f6d8`, 2026-08).

## License

Apache-2.0 WITH LLVM-exception, matching Spin.
