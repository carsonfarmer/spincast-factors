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

Spin is a pinned git dependency; the crates use only interfaces Spin
already ships on `main`.

## License

Apache-2.0 WITH LLVM-exception, matching Spin.
