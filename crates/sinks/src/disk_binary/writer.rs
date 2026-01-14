//! Binary file format writer and reader utilities
//!
//! This module defines the binary file format and provides tools for
//! writing and reading binary log files.
//!
//! # File Format
//!
//! Each message is stored as:
//! ```text
//! [24-byte Metadata][4-byte size][FlatBuffer data]
//! ```
//!
//! The metadata layout (24 bytes total):
//! - `batch_timestamp`: u64 (8 bytes) - Unix milliseconds when sink processed
//! - `source_ip`: [u8; 16] (16 bytes) - Client IP in IPv6 format

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use lz4_flex::frame::FrameDecoder;

/// Size of the metadata header in bytes
pub const METADATA_SIZE: usize = 24;

/// Size of the message length field in bytes
pub const LENGTH_FIELD_SIZE: usize = 4;

/// 24-byte metadata header for each message
///
/// This struct is NOT used directly for serialization (we use manual byte
/// writing for performance). It's provided for documentation and testing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageMetadata {
    /// When sink processed it (Unix milliseconds)
    pub batch_timestamp: u64,

    /// Client IP in IPv6 format (IPv4 is mapped as ::ffff:x.x.x.x)
    pub source_ip: [u8; 16],
}

impl MessageMetadata {
    /// Create a new metadata header
    pub fn new(batch_timestamp: u64, source_ip: [u8; 16]) -> Self {
        Self {
            batch_timestamp,
            source_ip,
        }
    }

    /// Serialize metadata to bytes (big-endian)
    pub fn to_bytes(&self) -> [u8; METADATA_SIZE] {
        let mut bytes = [0u8; METADATA_SIZE];
        bytes[0..8].copy_from_slice(&self.batch_timestamp.to_be_bytes());
        bytes[8..24].copy_from_slice(&self.source_ip);
        bytes
    }

    /// Deserialize metadata from bytes (big-endian)
    pub fn from_bytes(bytes: &[u8; METADATA_SIZE]) -> Self {
        let batch_timestamp = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let mut source_ip = [0u8; 16];
        source_ip.copy_from_slice(&bytes[8..24]);

        Self {
            batch_timestamp,
            source_ip,
        }
    }

    /// Get source IP as a string
    pub fn source_ip_string(&self) -> String {
        // Check if it's an IPv4-mapped address (::ffff:x.x.x.x)
        if self.source_ip[0..10] == [0u8; 10] && self.source_ip[10..12] == [0xff, 0xff] {
            format!(
                "{}.{}.{}.{}",
                self.source_ip[12], self.source_ip[13], self.source_ip[14], self.source_ip[15]
            )
        } else {
            // Full IPv6 format
            let segments: Vec<String> = (0..8)
                .map(|i| {
                    let high = self.source_ip[i * 2] as u16;
                    let low = self.source_ip[i * 2 + 1] as u16;
                    format!("{:x}", (high << 8) | low)
                })
                .collect();
            segments.join(":")
        }
    }
}

/// A message read from a binary file
#[derive(Debug, Clone)]
pub struct BinaryMessage {
    /// Metadata header
    pub metadata: MessageMetadata,

    /// FlatBuffer message data
    pub data: Vec<u8>,
}

/// Reader for binary log files
///
/// Supports both compressed (.bin.lz4) and uncompressed (.bin) files.
/// Provides sequential reading and time-range scanning.
pub struct BinaryReader {
    reader: Box<dyn Read>,
    is_compressed: bool,
}

impl BinaryReader {
    /// Open a binary file for reading
    ///
    /// Automatically detects LZ4 compression based on file extension.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let is_compressed = path.to_string_lossy().ends_with(".lz4");

        let file = File::open(path)?;
        let buf_reader = BufReader::with_capacity(32 * 1024, file);

        let reader: Box<dyn Read> = if is_compressed {
            Box::new(FrameDecoder::new(buf_reader))
        } else {
            Box::new(buf_reader)
        };

        Ok(Self {
            reader,
            is_compressed,
        })
    }

    /// Check if the file is LZ4 compressed
    pub fn is_compressed(&self) -> bool {
        self.is_compressed
    }

    /// Read the next message from the file
    ///
    /// Returns `None` at end of file, `Err` on I/O error.
    pub fn read_message(&mut self) -> io::Result<Option<BinaryMessage>> {
        // Read metadata (24 bytes)
        let mut meta_bytes = [0u8; METADATA_SIZE];
        match self.reader.read_exact(&mut meta_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let metadata = MessageMetadata::from_bytes(&meta_bytes);

        // Read message length (4 bytes, big-endian)
        let mut len_bytes = [0u8; LENGTH_FIELD_SIZE];
        self.reader.read_exact(&mut len_bytes)?;
        let msg_len = u32::from_be_bytes(len_bytes) as usize;

        // Read message data
        let mut data = vec![0u8; msg_len];
        self.reader.read_exact(&mut data)?;

        Ok(Some(BinaryMessage { metadata, data }))
    }

    /// Read all messages from the file
    pub fn read_all(&mut self) -> io::Result<Vec<BinaryMessage>> {
        let mut messages = Vec::new();
        while let Some(msg) = self.read_message()? {
            messages.push(msg);
        }
        Ok(messages)
    }

    /// Iterate over messages in the file
    pub fn messages(self) -> MessageIterator {
        MessageIterator { reader: self }
    }
}

/// Iterator over messages in a binary file
pub struct MessageIterator {
    reader: BinaryReader,
}

impl Iterator for MessageIterator {
    type Item = io::Result<BinaryMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_message() {
            Ok(Some(msg)) => Some(Ok(msg)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// Seekable reader for time-range scanning (uncompressed files only)
pub struct SeekableBinaryReader {
    reader: BufReader<File>,
    file_size: u64,
}

#[allow(dead_code)]
impl SeekableBinaryReader {
    /// Open an uncompressed binary file for seekable reading
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let reader = BufReader::with_capacity(32 * 1024, file);

        Ok(Self { reader, file_size })
    }

    /// Read the next message from the current position
    pub fn read_message(&mut self) -> io::Result<Option<BinaryMessage>> {
        // Check if we're at end of file
        if self.reader.stream_position()? >= self.file_size {
            return Ok(None);
        }

        // Read metadata (24 bytes)
        let mut meta_bytes = [0u8; METADATA_SIZE];
        match self.reader.read_exact(&mut meta_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let metadata = MessageMetadata::from_bytes(&meta_bytes);

        // Read message length (4 bytes, big-endian)
        let mut len_bytes = [0u8; LENGTH_FIELD_SIZE];
        self.reader.read_exact(&mut len_bytes)?;
        let msg_len = u32::from_be_bytes(len_bytes) as usize;

        // Read message data
        let mut data = vec![0u8; msg_len];
        self.reader.read_exact(&mut data)?;

        Ok(Some(BinaryMessage { metadata, data }))
    }

    /// Skip the next message without reading data
    ///
    /// Useful for fast time-range scanning.
    pub fn skip_message(&mut self) -> io::Result<Option<MessageMetadata>> {
        // Check if we're at end of file
        if self.reader.stream_position()? >= self.file_size {
            return Ok(None);
        }

        // Read metadata (24 bytes)
        let mut meta_bytes = [0u8; METADATA_SIZE];
        match self.reader.read_exact(&mut meta_bytes) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let metadata = MessageMetadata::from_bytes(&meta_bytes);

        // Read message length (4 bytes, big-endian)
        let mut len_bytes = [0u8; LENGTH_FIELD_SIZE];
        self.reader.read_exact(&mut len_bytes)?;
        let msg_len = u32::from_be_bytes(len_bytes) as usize;

        // Skip message data
        self.reader.seek(SeekFrom::Current(msg_len as i64))?;

        Ok(Some(metadata))
    }

    /// Scan messages in a time range
    ///
    /// Efficiently skips messages outside the time range.
    pub fn scan_time_range(
        &mut self,
        start_time: u64,
        end_time: u64,
    ) -> io::Result<Vec<BinaryMessage>> {
        // Reset to beginning
        self.reader.seek(SeekFrom::Start(0))?;

        let mut messages = Vec::new();

        loop {
            // Peek at metadata to check timestamp
            let pos = self.reader.stream_position()?;
            if pos >= self.file_size {
                break;
            }

            // Read metadata
            let mut meta_bytes = [0u8; METADATA_SIZE];
            match self.reader.read_exact(&mut meta_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let metadata = MessageMetadata::from_bytes(&meta_bytes);

            // Read message length
            let mut len_bytes = [0u8; LENGTH_FIELD_SIZE];
            self.reader.read_exact(&mut len_bytes)?;
            let msg_len = u32::from_be_bytes(len_bytes) as usize;

            // Check if timestamp is in range
            if metadata.batch_timestamp >= start_time && metadata.batch_timestamp <= end_time {
                // Read message data
                let mut data = vec![0u8; msg_len];
                self.reader.read_exact(&mut data)?;
                messages.push(BinaryMessage { metadata, data });
            } else {
                // Skip message data
                self.reader.seek(SeekFrom::Current(msg_len as i64))?;
            }
        }

        Ok(messages)
    }

    /// Get the current position in the file
    pub fn position(&mut self) -> io::Result<u64> {
        self.reader.stream_position()
    }

    /// Seek to a position in the file
    pub fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.reader.seek(pos)
    }

    /// Get the file size
    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_metadata_size() {
        assert_eq!(METADATA_SIZE, 24);
    }

    #[test]
    fn test_metadata_roundtrip() {
        let metadata = MessageMetadata::new(
            1234567890123,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1],
        );

        let bytes = metadata.to_bytes();
        let decoded = MessageMetadata::from_bytes(&bytes);

        assert_eq!(metadata, decoded);
    }

    #[test]
    fn test_metadata_ipv4_string() {
        let metadata = MessageMetadata::new(
            0,
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 1],
        );

        assert_eq!(metadata.source_ip_string(), "192.168.1.1");
    }

    #[test]
    fn test_metadata_ipv6_string() {
        let metadata = MessageMetadata::new(
            0,
            [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        );

        assert_eq!(metadata.source_ip_string(), "2001:db8:0:0:0:0:0:1");
    }

    #[test]
    fn test_read_write_roundtrip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write some messages
        {
            let mut file = File::create(path).unwrap();

            let metadata = MessageMetadata::new(
                1000,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 1],
            );
            let data = b"hello world";

            file.write_all(&metadata.to_bytes()).unwrap();
            file.write_all(&(data.len() as u32).to_be_bytes()).unwrap();
            file.write_all(data).unwrap();

            let metadata2 = MessageMetadata::new(
                2000,
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 10, 0, 0, 2],
            );
            let data2 = b"goodbye world";

            file.write_all(&metadata2.to_bytes()).unwrap();
            file.write_all(&(data2.len() as u32).to_be_bytes()).unwrap();
            file.write_all(data2).unwrap();
        }

        // Read messages back
        let mut reader = BinaryReader::open(path).unwrap();
        let messages = reader.read_all().unwrap();

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].metadata.batch_timestamp, 1000);
        assert_eq!(messages[0].data, b"hello world");
        assert_eq!(messages[1].metadata.batch_timestamp, 2000);
        assert_eq!(messages[1].data, b"goodbye world");
    }

    #[test]
    fn test_message_iterator() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write messages
        {
            let mut file = File::create(path).unwrap();

            for i in 0..5 {
                let metadata = MessageMetadata::new(i * 1000, [0u8; 16]);
                let data = format!("message {}", i);

                file.write_all(&metadata.to_bytes()).unwrap();
                file.write_all(&(data.len() as u32).to_be_bytes()).unwrap();
                file.write_all(data.as_bytes()).unwrap();
            }
        }

        // Read using iterator
        let reader = BinaryReader::open(path).unwrap();
        let messages: Vec<_> = reader.messages().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(messages.len(), 5);
        for (i, msg) in messages.iter().enumerate() {
            assert_eq!(msg.metadata.batch_timestamp, (i as u64) * 1000);
            assert_eq!(msg.data, format!("message {}", i).as_bytes());
        }
    }

    #[test]
    fn test_seekable_reader_time_range() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write messages with different timestamps
        {
            let mut file = File::create(path).unwrap();

            for i in 0..10 {
                let metadata = MessageMetadata::new(i * 100, [0u8; 16]);
                let data = format!("msg{}", i);

                file.write_all(&metadata.to_bytes()).unwrap();
                file.write_all(&(data.len() as u32).to_be_bytes()).unwrap();
                file.write_all(data.as_bytes()).unwrap();
            }
        }

        // Scan for time range 300-600
        let mut reader = SeekableBinaryReader::open(path).unwrap();
        let messages = reader.scan_time_range(300, 600).unwrap();

        assert_eq!(messages.len(), 4); // timestamps 300, 400, 500, 600
        assert_eq!(messages[0].metadata.batch_timestamp, 300);
        assert_eq!(messages[3].metadata.batch_timestamp, 600);
    }

    #[test]
    fn test_skip_message() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Write messages
        {
            let mut file = File::create(path).unwrap();

            for i in 0..3 {
                let metadata = MessageMetadata::new(i * 1000, [0u8; 16]);
                let data = vec![0u8; 100]; // 100 bytes each

                file.write_all(&metadata.to_bytes()).unwrap();
                file.write_all(&(data.len() as u32).to_be_bytes()).unwrap();
                file.write_all(&data).unwrap();
            }
        }

        // Skip first two, read third
        let mut reader = SeekableBinaryReader::open(path).unwrap();

        let meta1 = reader.skip_message().unwrap().unwrap();
        assert_eq!(meta1.batch_timestamp, 0);

        let meta2 = reader.skip_message().unwrap().unwrap();
        assert_eq!(meta2.batch_timestamp, 1000);

        let msg3 = reader.read_message().unwrap().unwrap();
        assert_eq!(msg3.metadata.batch_timestamp, 2000);
        assert_eq!(msg3.data.len(), 100);

        // Should be at end
        assert!(reader.read_message().unwrap().is_none());
    }

    #[test]
    fn test_empty_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        let mut reader = BinaryReader::open(path).unwrap();
        let messages = reader.read_all().unwrap();

        assert!(messages.is_empty());
    }
}
