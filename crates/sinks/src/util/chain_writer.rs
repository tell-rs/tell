//! Chain writers for different output formats
//!
//! Provides a trait abstraction for pluggable writers that can be used
//! with the atomic rotation system. Each writer wraps a file and provides
//! buffered writing with optional compression.
//!
//! # Available Writers
//!
//! - `PlainTextWriter` - Buffered text output (no compression)
//! - `Lz4Writer` - LZ4 compressed output with configurable settings
//! - `BinaryWriter` - Binary output with optional LZ4 compression
//!
//! # LZ4 Configuration
//!
//! The LZ4 writers support configuration matching the Go implementation:
//! - Block size: Max4MB (matches Go's `lz4.Block4Mb`)
//! - Content checksum: Enabled for data integrity
//!
//! Note: `lz4_flex` is a single-threaded implementation (Go uses 4-thread parallel).
//! For high-throughput scenarios, use multiple sinks or consider the Arrow sink.
//!
//! # Example
//!
//! ```ignore
//! use std::fs::File;
//! use sinks::util::chain_writer::{ChainWriter, PlainTextWriter, Lz4Config};
//!
//! let writer = PlainTextWriter::new(32 * 1024 * 1024);
//! let file = File::create("output.txt")?;
//! let mut chain = writer.wrap(file)?;
//!
//! chain.write_all(b"hello world")?;
//! chain.flush()?;
//! ```

use lz4_flex::frame::{BlockSize, FrameEncoder, FrameInfo};
use std::fs::File;
use std::io::{self, BufWriter, Write};

/// Default buffer size for writers (32MB for high throughput)
pub const DEFAULT_BUFFER_SIZE: usize = 32 * 1024 * 1024;

// ============================================================================
// LZ4 Configuration (matching Go implementation)
// ============================================================================

/// LZ4 compression configuration
///
/// Matches Go implementation settings:
/// - Block size: 4MB (Go: `lz4.Block4Mb`)
/// - Content checksum: true (data integrity)
///
/// Note: Go uses `lz4.ConcurrencyOption(4)` for parallel compression,
/// but `lz4_flex` is single-threaded. For high throughput, use multiple
/// sink instances or the Arrow-based ClickHouse sink.
#[derive(Debug, Clone, Copy)]
pub struct Lz4Config {
    /// Block size for compression (default: 4MB to match Go)
    pub block_size: Lz4BlockSize,
    /// Enable content checksum for integrity verification
    pub content_checksum: bool,
    /// Enable per-block checksums
    pub block_checksums: bool,
}

impl Default for Lz4Config {
    fn default() -> Self {
        Self {
            // Match Go: lz4.BlockSizeOption(lz4.Block4Mb)
            block_size: Lz4BlockSize::Max4MB,
            // Enable content checksum for data integrity
            content_checksum: true,
            // Disable per-block checksums (Go doesn't use them)
            block_checksums: false,
        }
    }
}

impl Lz4Config {
    /// Create optimized config matching Go implementation
    pub fn go_compatible() -> Self {
        Self::default()
    }

    /// Create config optimized for maximum speed (smaller blocks)
    pub fn fast() -> Self {
        Self {
            block_size: Lz4BlockSize::Max256KB,
            content_checksum: false,
            block_checksums: false,
        }
    }

    /// Create config optimized for compression ratio (larger blocks)
    pub fn max_compression() -> Self {
        Self {
            block_size: Lz4BlockSize::Max8MB,
            content_checksum: true,
            block_checksums: false,
        }
    }

    /// Build FrameInfo from this config
    fn to_frame_info(self) -> FrameInfo {
        FrameInfo::new()
            .block_size(self.block_size.into())
            .content_checksum(self.content_checksum)
            .block_checksums(self.block_checksums)
    }
}

/// LZ4 block size options
///
/// Larger blocks = better compression ratio but more memory
/// Smaller blocks = faster compression but worse ratio
#[derive(Debug, Clone, Copy, Default)]
pub enum Lz4BlockSize {
    /// 64KB blocks (fastest, worst compression)
    Max64KB,
    /// 256KB blocks
    Max256KB,
    /// 1MB blocks
    Max1MB,
    /// 4MB blocks (Go default, good balance)
    #[default]
    Max4MB,
    /// 8MB blocks (best compression, most memory)
    Max8MB,
}

impl From<Lz4BlockSize> for BlockSize {
    fn from(size: Lz4BlockSize) -> Self {
        match size {
            Lz4BlockSize::Max64KB => BlockSize::Max64KB,
            Lz4BlockSize::Max256KB => BlockSize::Max256KB,
            Lz4BlockSize::Max1MB => BlockSize::Max1MB,
            Lz4BlockSize::Max4MB => BlockSize::Max4MB,
            Lz4BlockSize::Max8MB => BlockSize::Max8MB,
        }
    }
}

/// Trait for pluggable chain writers
///
/// Implementations wrap a file and provide buffered writing with optional
/// compression. The writer owns the underlying file and handles flushing.
pub trait ChainWriter: Send + Sync {
    /// Wrap a file with this writer's buffering/compression strategy
    fn wrap(&self, file: File) -> io::Result<Box<dyn ChainWrite>>;

    /// Get the file extension for this writer type
    fn file_extension(&self) -> &'static str;
}

/// Trait for the actual write operations
///
/// This is object-safe and can be used with `Box<dyn ChainWrite>`.
pub trait ChainWrite: Write + Send {
    /// Flush all buffered data to the underlying file
    fn flush_all(&mut self) -> io::Result<()>;

    /// Finish writing and close the writer (for compression finalization)
    fn finish(self: Box<Self>) -> io::Result<()>;

    /// Get the number of bytes written to the underlying file
    fn bytes_written(&self) -> u64;
}

// ============================================================================
// PlainTextWriter - Buffered text output
// ============================================================================

/// Plain text writer with buffering (no compression)
///
/// Uses `BufWriter` for efficient buffered I/O. Suitable for human-readable
/// log files where compression isn't needed.
#[derive(Debug, Clone)]
pub struct PlainTextWriter {
    buffer_size: usize,
}

impl PlainTextWriter {
    /// Create a new plain text writer with the specified buffer size
    pub fn new(buffer_size: usize) -> Self {
        Self { buffer_size }
    }
}

impl Default for PlainTextWriter {
    fn default() -> Self {
        Self::new(DEFAULT_BUFFER_SIZE)
    }
}

impl ChainWriter for PlainTextWriter {
    fn wrap(&self, file: File) -> io::Result<Box<dyn ChainWrite>> {
        Ok(Box::new(PlainTextChain {
            writer: BufWriter::with_capacity(self.buffer_size, file),
            bytes_written: 0,
        }))
    }

    fn file_extension(&self) -> &'static str {
        ".txt"
    }
}

struct PlainTextChain {
    writer: BufWriter<File>,
    bytes_written: u64,
}

impl Write for PlainTextChain {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl ChainWrite for PlainTextChain {
    fn flush_all(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    fn finish(mut self: Box<Self>) -> io::Result<()> {
        self.writer.flush()
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

// ============================================================================
// Lz4Writer - LZ4 compressed output
// ============================================================================

/// LZ4 compressed writer with configurable settings
///
/// Uses LZ4 frame compression with settings matching Go implementation:
/// - 4MB block size for good compression ratio
/// - Content checksum for data integrity
///
/// Suitable for archival storage where space matters.
#[derive(Debug, Clone)]
pub struct Lz4Writer {
    buffer_size: usize,
    lz4_config: Lz4Config,
}

impl Lz4Writer {
    /// Create a new LZ4 writer with the specified buffer size and default config
    pub fn new(buffer_size: usize) -> Self {
        Self {
            buffer_size,
            lz4_config: Lz4Config::default(),
        }
    }

    /// Create a new LZ4 writer with custom compression config
    pub fn with_config(buffer_size: usize, lz4_config: Lz4Config) -> Self {
        Self {
            buffer_size,
            lz4_config,
        }
    }

    /// Create a Go-compatible LZ4 writer (4MB blocks, content checksum)
    pub fn go_compatible(buffer_size: usize) -> Self {
        Self::with_config(buffer_size, Lz4Config::go_compatible())
    }
}

impl Default for Lz4Writer {
    fn default() -> Self {
        Self::new(DEFAULT_BUFFER_SIZE)
    }
}

impl ChainWriter for Lz4Writer {
    fn wrap(&self, file: File) -> io::Result<Box<dyn ChainWrite>> {
        let buf_writer = BufWriter::with_capacity(self.buffer_size, file);
        let frame_info = self.lz4_config.to_frame_info();
        let encoder = FrameEncoder::with_frame_info(frame_info, buf_writer);
        Ok(Box::new(Lz4Chain {
            encoder,
            bytes_written: 0,
        }))
    }

    fn file_extension(&self) -> &'static str {
        ".lz4"
    }
}

struct Lz4Chain {
    encoder: FrameEncoder<BufWriter<File>>,
    bytes_written: u64,
}

impl Write for Lz4Chain {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.encoder.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.encoder.flush()
    }
}

impl ChainWrite for Lz4Chain {
    fn flush_all(&mut self) -> io::Result<()> {
        self.encoder.flush()
    }

    fn finish(self: Box<Self>) -> io::Result<()> {
        // Finish the LZ4 frame and flush the underlying writer
        let buf_writer = self.encoder.finish()?;
        // BufWriter is already flushed by FrameEncoder::finish()
        drop(buf_writer);
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

// ============================================================================
// BinaryWriter - Binary output with optional compression
// ============================================================================

/// Binary writer with optional LZ4 compression
///
/// Used for disk_binary sink. Supports both compressed and uncompressed
/// binary output based on configuration. When compression is enabled,
/// uses Go-compatible LZ4 settings (4MB blocks, content checksum).
#[derive(Debug, Clone)]
pub struct BinaryWriter {
    buffer_size: usize,
    use_compression: bool,
    lz4_config: Lz4Config,
}

impl BinaryWriter {
    /// Create a new binary writer
    pub fn new(buffer_size: usize, use_compression: bool) -> Self {
        Self {
            buffer_size,
            use_compression,
            lz4_config: Lz4Config::default(),
        }
    }

    /// Create a new binary writer with custom LZ4 config
    pub fn with_lz4_config(buffer_size: usize, lz4_config: Lz4Config) -> Self {
        Self {
            buffer_size,
            use_compression: true,
            lz4_config,
        }
    }

    /// Create an uncompressed binary writer
    pub fn uncompressed(buffer_size: usize) -> Self {
        Self::new(buffer_size, false)
    }

    /// Create a compressed binary writer with Go-compatible settings
    pub fn compressed(buffer_size: usize) -> Self {
        Self::new(buffer_size, true)
    }

    /// Create a compressed binary writer with custom LZ4 config
    pub fn compressed_with_config(buffer_size: usize, lz4_config: Lz4Config) -> Self {
        Self::with_lz4_config(buffer_size, lz4_config)
    }
}

impl Default for BinaryWriter {
    fn default() -> Self {
        Self::new(DEFAULT_BUFFER_SIZE, false)
    }
}

impl ChainWriter for BinaryWriter {
    fn wrap(&self, file: File) -> io::Result<Box<dyn ChainWrite>> {
        if self.use_compression {
            let buf_writer = BufWriter::with_capacity(self.buffer_size, file);
            let frame_info = self.lz4_config.to_frame_info();
            let encoder = FrameEncoder::with_frame_info(frame_info, buf_writer);
            Ok(Box::new(BinaryLz4Chain {
                encoder,
                bytes_written: 0,
            }))
        } else {
            Ok(Box::new(BinaryPlainChain {
                writer: BufWriter::with_capacity(self.buffer_size, file),
                bytes_written: 0,
            }))
        }
    }

    fn file_extension(&self) -> &'static str {
        if self.use_compression {
            ".bin.lz4"
        } else {
            ".bin"
        }
    }
}

struct BinaryPlainChain {
    writer: BufWriter<File>,
    bytes_written: u64,
}

impl Write for BinaryPlainChain {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.writer.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl ChainWrite for BinaryPlainChain {
    fn flush_all(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    fn finish(mut self: Box<Self>) -> io::Result<()> {
        self.writer.flush()
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

struct BinaryLz4Chain {
    encoder: FrameEncoder<BufWriter<File>>,
    bytes_written: u64,
}

impl Write for BinaryLz4Chain {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.encoder.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.encoder.flush()
    }
}

impl ChainWrite for BinaryLz4Chain {
    fn flush_all(&mut self) -> io::Result<()> {
        self.encoder.flush()
    }

    fn finish(self: Box<Self>) -> io::Result<()> {
        let buf_writer = self.encoder.finish()?;
        drop(buf_writer);
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
}

#[cfg(test)]
#[path = "chain_writer_test.rs"]
mod chain_writer_test;
