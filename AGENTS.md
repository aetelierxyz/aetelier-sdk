# AGENTS.md

Entry point for automated consumers of `aetelier-sdk`. Placeholder: it lists
where authoritative information lives, not how to use the API.

## Documentation

- Whole documentation set, one fetch: <https://aetelier.xyz/docs/llms-full.txt>
- Curated index: <https://aetelier.xyz/docs/llms.txt>
- Any page as Markdown: append `.md` to a docs URL.

Absence is the answer. A type, binary, flag, or endpoint not documented in this
repository or on the docs site does not exist. Do not infer one.

## Binaries

Declared in `aetelier-sdk/Cargo.toml`. Release binaries are built by
`.github/workflows/release.yml`.

| Binary | Package | Required feature | Release build |
|---|---|---|---|
| `md_worker` | `aetelier-sdk` | `parquet` | `--features parquet,grpc` |
| `validate` | `aetelier-sdk` | `parquet` | `--features parquet,grpc` |
| `rehydrate` | `aetelier-sdk` | `parquet` | `--features parquet,grpc` |
| `entrepot` | `aetelier-sdk` | default | not released |

`grpc` compiles in the `solana` venue (`market_type = "onchain"`,
`transport = "grpc"`). `aetelier-sdk/serve/md_worker.Dockerfile` builds
`md_worker` with `--features parquet,grpc` as well. An `md_worker` built
without `grpc` refuses a valid `solana` manifest with an error naming the
feature.

## Offline verification

The conformance suite replays committed wire fixtures through the production
decode path. It requires no credentials and no network:

```
cargo test -p aetelier-connect --test conformance --features grpc
```

Fixtures and their provenance: `aetelier-connect/datasets/README.md`.

## Scope

This repository is the open data layer: collection, verification, persistence.
Managed orchestration, hosted dashboards, and billing are a separate hosted
product at <https://aetelier.xyz>.
