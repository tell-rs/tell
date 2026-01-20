#!/bin/bash
# Build release artifacts for all platforms
# Usage: ./scripts/build-release.sh 2.0.0
#
# Prerequisites:
#   brew install zig
#   cargo install cargo-zigbuild
#   rustup target add x86_64-unknown-linux-gnu
#   rustup target add aarch64-unknown-linux-gnu
#   rustup target add x86_64-unknown-linux-musl
#   rustup target add aarch64-apple-darwin

set -e

VERSION="${1:-}"

if [ -z "$VERSION" ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 2.0.0"
    exit 1
fi

# Validate version format
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Version must be in format X.Y.Z (e.g., 2.0.0)"
    exit 1
fi

# Check Cargo.toml version matches
CARGO_VERSION=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
if [ "$CARGO_VERSION" != "$VERSION" ]; then
    echo "Error: Version $VERSION does not match Cargo.toml version $CARGO_VERSION"
    exit 1
fi

echo "Building Tell v$VERSION"
echo "================================"

# Setup
DIST_DIR="dist/v$VERSION"
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

# Targets
TARGETS=(
    "x86_64-unknown-linux-gnu:linux-amd64"
    "aarch64-unknown-linux-gnu:linux-arm64"
    "x86_64-unknown-linux-musl:linux-amd64-musl"
    "aarch64-apple-darwin:macos-aarch64"
)

# Build each target
for target_pair in "${TARGETS[@]}"; do
    RUST_TARGET="${target_pair%%:*}"
    ARTIFACT_SUFFIX="${target_pair##*:}"

    echo ""
    echo "Building for $RUST_TARGET..."

    if [[ "$RUST_TARGET" == *"apple"* ]]; then
        # Native macOS build
        cargo build --release --target "$RUST_TARGET" -p tell
    else
        # Cross-compile with zigbuild
        cargo zigbuild --release --target "$RUST_TARGET" -p tell
    fi

    BINARY="target/$RUST_TARGET/release/tell"

    if [ ! -f "$BINARY" ]; then
        echo "Error: Binary not found at $BINARY"
        exit 1
    fi

    # Strip binary (reduces size significantly)
    if [[ "$RUST_TARGET" == *"apple"* ]]; then
        strip "$BINARY"
    elif command -v llvm-strip &> /dev/null; then
        llvm-strip "$BINARY"
    fi

    # Package
    if [[ "$ARTIFACT_SUFFIX" == "macos"* ]]; then
        # macOS: raw binary
        ARTIFACT_NAME="tell-$ARTIFACT_SUFFIX"
        cp "$BINARY" "$DIST_DIR/$ARTIFACT_NAME"
        echo "Created: $ARTIFACT_NAME"
    else
        # Linux: tarball
        ARCHIVE_NAME="tell-$VERSION-$ARTIFACT_SUFFIX"
        ARCHIVE_DIR="$DIST_DIR/$ARCHIVE_NAME"

        mkdir -p "$ARCHIVE_DIR"
        cp "$BINARY" "$ARCHIVE_DIR/tell"
        cp LICENSE "$ARCHIVE_DIR/" 2>/dev/null || echo "Warning: LICENSE not found"
        cp README.md "$ARCHIVE_DIR/" 2>/dev/null || echo "Warning: README.md not found"

        # Create tarball
        (cd "$DIST_DIR" && tar -czf "$ARCHIVE_NAME.tgz" "$ARCHIVE_NAME")
        rm -rf "$ARCHIVE_DIR"

        # Create checksum
        (cd "$DIST_DIR" && shasum -a 512 "$ARCHIVE_NAME.tgz" | cut -d' ' -f1 > "$ARCHIVE_NAME.tgz.sha512")

        echo "Created: $ARCHIVE_NAME.tgz"
    fi
done

echo ""
echo "================================"
echo "Build complete! Artifacts in $DIST_DIR:"
echo ""
ls -la "$DIST_DIR"

echo ""
echo "To create GitHub release:"
echo "  git tag v$VERSION"
echo "  git push origin v$VERSION"
echo "  gh release create v$VERSION --title \"v$VERSION\" $DIST_DIR/*"
