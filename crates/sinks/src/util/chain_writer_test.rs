//! Tests for chain writers

use crate::util::chain_writer::{
    BinaryWriter, ChainWriter, DEFAULT_BUFFER_SIZE, Lz4Writer, PlainTextWriter,
};
use std::io::Write;
use tempfile::NamedTempFile;

// ============================================================================
// PlainTextWriter Tests
// ============================================================================

#[test]
fn test_plaintext_writer_default() {
    let writer = PlainTextWriter::default();
    assert_eq!(writer.file_extension(), ".txt");
}

#[test]
fn test_plaintext_writer_custom_buffer() {
    let writer = PlainTextWriter::new(1024);
    assert_eq!(writer.file_extension(), ".txt");
}

#[test]
fn test_plaintext_writer_wrap_and_write() {
    let writer = PlainTextWriter::new(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let mut chain = writer.wrap(file).unwrap();
    chain.write_all(b"hello world\n").unwrap();
    chain.flush_all().unwrap();

    assert_eq!(chain.bytes_written(), 12);
}

#[test]
fn test_plaintext_writer_finish() {
    let writer = PlainTextWriter::new(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let mut chain = writer.wrap(file).unwrap();
    chain.write_all(b"test data").unwrap();
    chain.finish().unwrap();

    // Verify file content
    let content = std::fs::read(temp_file.path()).unwrap();
    assert_eq!(content, b"test data");
}

#[test]
fn test_plaintext_writer_multiple_writes() {
    let writer = PlainTextWriter::new(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let mut chain = writer.wrap(file).unwrap();

    for i in 0..10 {
        let line = format!("line {}\n", i);
        chain.write_all(line.as_bytes()).unwrap();
    }
    chain.finish().unwrap();

    let content = std::fs::read_to_string(temp_file.path()).unwrap();
    assert!(content.contains("line 0"));
    assert!(content.contains("line 9"));
}

// ============================================================================
// Lz4Writer Tests
// ============================================================================

#[test]
fn test_lz4_writer_default() {
    let writer = Lz4Writer::default();
    assert_eq!(writer.file_extension(), ".lz4");
}

#[test]
fn test_lz4_writer_custom_buffer() {
    let writer = Lz4Writer::new(1024);
    assert_eq!(writer.file_extension(), ".lz4");
}

#[test]
fn test_lz4_writer_wrap_and_write() {
    let writer = Lz4Writer::new(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let mut chain = writer.wrap(file).unwrap();
    chain.write_all(b"hello world\n").unwrap();
    chain.flush_all().unwrap();

    assert_eq!(chain.bytes_written(), 12);
}

#[test]
fn test_lz4_writer_finish_produces_valid_lz4() {
    let writer = Lz4Writer::new(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let mut chain = writer.wrap(file).unwrap();
    chain.write_all(b"test data for compression").unwrap();
    chain.finish().unwrap();

    // Verify the file has LZ4 magic bytes
    let content = std::fs::read(temp_file.path()).unwrap();
    // LZ4 frame magic: 0x184D2204
    assert!(content.len() >= 4);
    assert_eq!(&content[0..4], &[0x04, 0x22, 0x4D, 0x18]);
}

#[test]
fn test_lz4_writer_roundtrip() {
    use lz4_flex::frame::FrameDecoder;
    use std::io::Read;

    let writer = Lz4Writer::new(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let original = b"hello world, this is a test of lz4 compression";

    let mut chain = writer.wrap(file).unwrap();
    chain.write_all(original).unwrap();
    chain.finish().unwrap();

    // Read back and decompress
    let compressed = std::fs::read(temp_file.path()).unwrap();
    let mut decoder = FrameDecoder::new(&compressed[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();

    assert_eq!(decompressed, original);
}

// ============================================================================
// BinaryWriter Tests
// ============================================================================

#[test]
fn test_binary_writer_default() {
    let writer = BinaryWriter::default();
    assert_eq!(writer.file_extension(), ".bin");
}

#[test]
fn test_binary_writer_uncompressed() {
    let writer = BinaryWriter::uncompressed(4096);
    assert_eq!(writer.file_extension(), ".bin");
}

#[test]
fn test_binary_writer_compressed() {
    let writer = BinaryWriter::compressed(4096);
    assert_eq!(writer.file_extension(), ".bin.lz4");
}

#[test]
fn test_binary_writer_uncompressed_write() {
    let writer = BinaryWriter::uncompressed(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let binary_data: Vec<u8> = (0..256).map(|i| i as u8).collect();

    let mut chain = writer.wrap(file).unwrap();
    chain.write_all(&binary_data).unwrap();
    chain.finish().unwrap();

    // Verify exact binary content
    let content = std::fs::read(temp_file.path()).unwrap();
    assert_eq!(content, binary_data);
}

#[test]
fn test_binary_writer_compressed_write() {
    use lz4_flex::frame::FrameDecoder;
    use std::io::Read;

    let writer = BinaryWriter::compressed(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let binary_data: Vec<u8> = (0..256).map(|i| i as u8).collect();

    let mut chain = writer.wrap(file).unwrap();
    chain.write_all(&binary_data).unwrap();
    chain.finish().unwrap();

    // Decompress and verify
    let compressed = std::fs::read(temp_file.path()).unwrap();
    let mut decoder = FrameDecoder::new(&compressed[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();

    assert_eq!(decompressed, binary_data);
}

#[test]
fn test_binary_writer_bytes_written() {
    let writer = BinaryWriter::uncompressed(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let mut chain = writer.wrap(file).unwrap();

    chain.write_all(&[0u8; 100]).unwrap();
    assert_eq!(chain.bytes_written(), 100);

    chain.write_all(&[1u8; 50]).unwrap();
    assert_eq!(chain.bytes_written(), 150);

    chain.finish().unwrap();
}

// ============================================================================
// Default Buffer Size Tests
// ============================================================================

#[test]
fn test_default_buffer_size() {
    // 32MB
    assert_eq!(DEFAULT_BUFFER_SIZE, 32 * 1024 * 1024);
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_empty_write() {
    let writer = PlainTextWriter::new(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let mut chain = writer.wrap(file).unwrap();
    chain.write_all(b"").unwrap();
    chain.finish().unwrap();

    let content = std::fs::read(temp_file.path()).unwrap();
    assert!(content.is_empty());
}

#[test]
fn test_large_write() {
    let writer = PlainTextWriter::new(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    // Write 1MB of data
    let large_data = vec![b'x'; 1024 * 1024];

    let mut chain = writer.wrap(file).unwrap();
    chain.write_all(&large_data).unwrap();
    chain.finish().unwrap();

    let content = std::fs::read(temp_file.path()).unwrap();
    assert_eq!(content.len(), 1024 * 1024);
}

#[test]
fn test_flush_multiple_times() {
    let writer = PlainTextWriter::new(4096);
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    let mut chain = writer.wrap(file).unwrap();

    chain.write_all(b"first").unwrap();
    chain.flush_all().unwrap();

    chain.write_all(b"second").unwrap();
    chain.flush_all().unwrap();

    chain.write_all(b"third").unwrap();
    chain.finish().unwrap();

    let content = std::fs::read_to_string(temp_file.path()).unwrap();
    assert_eq!(content, "firstsecondthird");
}
