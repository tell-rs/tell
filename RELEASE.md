# Release Guide

## Quick Release

```bash
# 1. Update version in Cargo.toml
# 2. Update CHANGELOG.md
# 3. Commit and tag
git add -A && git commit -m "Release v2.0.0"
git tag v2.0.0
git push origin main --tags
# 4. WarpBuild CI builds and creates GitHub release automatically
```

## Manual Release (Local)

```bash
./scripts/build-release.sh 2.0.0
gh release create v2.0.0 --title "v2.0.0" dist/v2.0.0/*
```

## Artifacts

| Platform | Artifact |
|----------|----------|
| Linux x86_64 | `tell-{v}-linux-amd64.tgz` + `.sha512` |
| Linux ARM64 | `tell-{v}-linux-arm64.tgz` + `.sha512` |
| Linux static | `tell-{v}-linux-amd64-musl.tgz` + `.sha512` |
| macOS ARM | `tell-macos-aarch64` |

## Prerequisites (Local)

```bash
brew install zig
cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-unknown-linux-musl aarch64-apple-darwin
```

## CI

- **Workflow:** `.github/workflows/release.yml`
- **Runner:** WarpBuild (`warp-ubuntu-2404-x64-4x`)
- **Trigger:** Push tag `v*`
- **Tool:** cargo-zigbuild (all targets from Linux)

## Install Script

```bash
curl -fsSL https://raw.githubusercontent.com/tell-rs/tell/main/install.sh | sh
```
