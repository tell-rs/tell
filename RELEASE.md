# Release Guide

## Quick Release

```bash
# 1. Update version in Cargo.toml
# 2. Update CHANGELOG.md
# 3. Commit and tag
git add -A && git commit -m "Release v2.0.0"
git tag v2.0.0
git push origin main --tags
# 4. GitHub Actions builds and creates release automatically
```

## Manual Release (Local Build)

Build all artifacts locally and create release:

```bash
./scripts/build-release.sh 2.0.0
gh release create v2.0.0 --title "v2.0.0" --generate-notes dist/v2.0.0/*
```

**Note:** macOS builds require running on a Mac. Linux builds use zigbuild (cross-compile from any platform).

## Upload to Existing Release

If CI built Linux artifacts but you need to add macOS manually:

```bash
# Build macOS locally (on your Mac)
cargo build --release -p tell
strip target/release/tell

# Upload to existing release (renames file on upload)
gh release upload v2.0.0 "target/release/tell#tell-macos-aarch64"
```

Or build just macOS with the script:

```bash
# This only works on macOS
cargo build --release --target aarch64-apple-darwin -p tell
strip target/aarch64-apple-darwin/release/tell
gh release upload v2.0.0 "target/aarch64-apple-darwin/release/tell#tell-macos-aarch64"
```

## Artifacts

| Platform | Artifact | Notes |
|----------|----------|-------|
| Linux x86_64 | `tell-{v}-linux-amd64.tgz` + `.sha512` | glibc |
| Linux ARM64 | `tell-{v}-linux-arm64.tgz` + `.sha512` | glibc |
| Linux static | `tell-{v}-linux-amd64-musl.tgz` + `.sha512` | musl, fully static |
| macOS ARM | `tell-macos-aarch64` | M1/M2/M3/M4 |

## Prerequisites (Local Builds)

```bash
# On macOS
brew install zig
cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu x86_64-unknown-linux-musl aarch64-apple-darwin
```

## CI

- **Workflow:** `.github/workflows/release.yml`
- **Runner:** `blacksmith-4vcpu-ubuntu-2404` (Blacksmith)
- **Trigger:** Push tag `v*`
- **Cross-compile:** cargo-zigbuild for all targets (Linux + macOS ARM64)
- **Caching:** sccache + rust-cache for faster builds

## Install Script

Users install via:

```bash
curl -fsSL https://tell.rs | sh
# or
curl -fsSL https://raw.githubusercontent.com/tell-rs/tell/main/install.sh | sh
```
