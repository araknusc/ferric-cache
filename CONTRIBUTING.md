# Contributing to ferric-cache

Thanks for your interest in contributing! This document covers how to build,
test, and submit changes.

## Repository layout

The Cargo crate lives in [`cache/`](cache/). All build/test/run commands are
issued from that directory. See [`ARCHITECTURE.md`](ARCHITECTURE.md) for a
tour of the code and the conventions for adding commands or subsystems.

## Prerequisites

- Rust 1.78 or newer (`rustup` recommended).
- A C toolchain (`cc`/`gcc`/MSVC) — `mlua` vendors Lua 5.4 and builds it from
  source, so a working C compiler is required.
- Optionally `openssl` (for `generate_certs.*`) and `redis-server` +
  `redis-benchmark` (for the comparative benchmarks in `docs/`).

## Build & test

```bash
cd cache
cargo build
cargo test              # unit + integration tests (integration tests bind real TCP ports)
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Integration tests bind real TCP ports; if you run several suites in parallel,
give them distinct ports.

## Pull requests

1. Fork and create a topic branch.
2. Keep changes focused; match the surrounding code style.
3. Ensure `cargo test`, `cargo clippy`, and `cargo fmt --check` pass.
4. Add or update tests for behavior changes.
5. Describe the change and its motivation in the PR body.

## Reporting security issues

Please do not open public issues for security vulnerabilities. Email the
maintainer directly so a fix can be prepared before disclosure.

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE).

## Conduct and security

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md). Security
issues should be reported privately as described in [SECURITY.md](SECURITY.md).
