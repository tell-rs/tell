//! Chain writers for different output formats
//!
//! Provides a trait abstraction for pluggable writers that can be used
//! with the atomic rotation system. Each writer wraps a file and provides
//! buffered writing with optional compression.
//!
//! # Available Writers
//!
//! - `PlainTextWriter` - Buffered text output (no compression)
//! - `Lz4Writer` - LZ4 compressed output
//! - `BinaryWriter` - Binary output with optional LZ4 compression
//!
//! # Example
//!
//! ```ignore
//! use std::fs::File;
//! use sinks::util::chain_writer::{ChainWriter, PlainTextWriter};
//!
//! let writer = PlainTextWriter::new(32 * 1024 * 1024);
//! let file = File::create("output.txt")?;
//! let mut chain = writer.wrap(file)?;
//!
//! chain.write_all(b"hello world")?;
//! chain.flush()?;
//! ```

use lz4_flex::frame::FrameEncoder;
use std::fs::File;
use std::io::{self, BufWriter, Write};

/// Default buffer size for writers (32MB for high throughput)
pub const DEFAULT_BUFFER_SIZE: usize = 32 * 1024 * 1024;

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

/// LZ4 compressed writer
///
/// Uses LZ4 frame compression for good compression ratio with fast
/// decompression. Suitable for archival storage where space matters.
#[derive(Debug, Clone)]
pub struct Lz4Writer {
    buffer_size: usize,
}

impl Lz4Writer {
    /// Create a new LZ4 writer with the specified buffer size
    pub fn new(buffer_size: usize) -> Self {
        Self { buffer_size }
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
        let encoder = FrameEncoder::new(buf_writer);
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
/// binary output based on configuration.
#[derive(Debug, Clone)]
pub struct BinaryWriter {
    buffer_size: usize,
    use_compression: bool,
}

impl BinaryWriter {
    /// Create a new binary writer
    pub fn new(buffer_size: usize, use_compression: bool) -> Self {
        Self {
            buffer_size,
            use_compression,
        }
    }

    /// Create an uncompressed binary writer
    pub fn uncompressed(buffer_size: usize) -> Self {
        Self::new(buffer_size, false)
    }

    /// Create a compressed binary writer
    pub fn compressed(buffer_size: usize) -> Self {
        Self::new(buffer_size, true)
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
            let encoder = FrameEncoder::new(buf_writer);
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
