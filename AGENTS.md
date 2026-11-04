# AGENTS.md

Rust libraries for Prometheus and OpenMetrics scrape pipelines. See [README.md](README.md) for scope and layout.

## Verify your change

Run these before you push. CI runs the same commands, see [.github/workflows/ci.yml](.github/workflows/ci.yml).

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo clippy --all-targets --no-default-features -- -D warnings
cargo test --all-features
cargo test --no-default-features
```

If you touch `src/arrow`, also run the OTAP compatibility suite. It has its own manifest:

```sh
cargo test --manifest-path tests/otap-compat/Cargo.toml
```

Do not use Rust language or std features newer than `rust-version` in `Cargo.toml`.

## Spec

[specs/expfmt/SPEC.md](specs/expfmt/SPEC.md) is the contract for `expfmt`. If you change behavior the spec describes, update the spec in the same PR.

## Test data

Do not edit files under `testdata/` by hand. They are generated from the Go modules pinned in `tests/go-harness/go.mod`. To regenerate them (requires Go):

```sh
cd tests/go-harness && go generate ./... && go test ./...
```

CI regenerates them and fails if `git diff --exit-code testdata` shows a change.

## Go is the reference

The Go code in `prometheus/prometheus` (`model/textparse`) and `prometheus/common` (`expfmt`) is the reference. Expected outputs in `testdata/` come from running the Go parsers. If Rust and Go disagree, fix the Rust code, not the fixture.

If Rust must differ from Go on purpose, explain each difference in the PR body with a permalink to the Go code.

## Commits and PRs

- Sign off every commit with `git commit -s` (DCO).
- PR titles use `<area>: <summary>`, for example `parse: add OpenMetrics 1.0 parser`.
- Link the GitHub issue the PR closes.
