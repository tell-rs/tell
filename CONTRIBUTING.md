# Contributing

## Setup

```bash
git config core.hooksPath .githooks
```

This auto-formats code on commit.

## Before Pushing

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Commands

```bash
cargo build          # build
cargo test           # test
cargo clippy         # lint
cargo fmt            # format
```
