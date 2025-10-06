use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;

// An in-memory representation of a data block
struct DataBlock {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    size: usize,
}

impl DataBlock {
    fn new() -> Self {
        DataBlock {
            entries: Vec::new(),
            size: 0,
        }
    }

    // Add a key-value pair to the block
    fn add(&mut self, key: &[u8], value: &[u8]) {
        // 4 bytes for key_len, 4 for value_len
        self.size += 8 + key.len() + value.len();
        self.entries.push((key.to_vec(), value.to_vec()));
    }
    
    // Get the last key in the block
    fn last_key(&self) -> Option<&[u8]> {
        self.entries.last().map(|(k, _)| k.as_slice())
    }

    // Serialise the block to bytes
    // Format: [num_entries: u32][key1_len: u32][key1][val1_len: u32][val1]...
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (key, value) in &self.entries {
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(key);
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
        }
        bytes
    }
}

// Represents an entry in the index block
// Format: [last_key_len: u32][last_key][block_offset: u64][block_size: u64]
struct IndexEntry {
    last_key: Vec<u8>,
    block_offset: u64,
    block_size: u64,
}

impl IndexEntry {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(self.last_key.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.last_key);
        bytes.extend_from_slice(&self.block_offset.to_le_bytes());
        bytes.extend_from_slice(&self.block_size.to_le_bytes());
        bytes
    }
}


/// Builds an SST file.
pub struct SstWriter {
    writer: BufWriter<File>,
    current_block: DataBlock,
    index: Vec<IndexEntry>,
    offset: u64,
    block_size_threshold: usize,
}

impl SstWriter {
    /// Creates a new writer for the given path.
    pub fn new(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        Ok(SstWriter {
            writer: BufWriter::new(file),
            current_block: DataBlock::new(),
            index: Vec::new(),
            offset: 0,
            block_size_threshold: 4096, // 4KB block size target
        })
    }

    /// Adds a key-value pair. Keys MUST be added in sorted order.
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.current_block.add(key, value);
        if self.current_block.size >= self.block_size_threshold {
            self.flush_block()?;
        }
        Ok(())
    }

    // Writes the current data block to the file
    fn flush_block(&mut self) -> io::Result<()> {
        if self.current_block.entries.is_empty() {
            return Ok(());
        }
        
        let last_key = self.current_block.last_key().unwrap().to_vec();
        let block_bytes = self.current_block.to_bytes();
        let block_size = block_bytes.len() as u64;

        self.writer.write_all(&block_bytes)?;

        self.index.push(IndexEntry {
            last_key,
            block_offset: self.offset,
            block_size,
        });

        self.offset += block_size;
        self.current_block = DataBlock::new();
        Ok(())
    }

    /// Finalizes the SST file by writing the index and footer.
    pub fn finish(mut self) -> io::Result<()> {
        // Flush any remaining data in the current block
        self.flush_block()?;
        
        // Write the index block
        let index_block_offset = self.offset;
        let mut index_bytes = Vec::new();
        index_bytes.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        for entry in &self.index {
            index_bytes.extend_from_slice(&entry.to_bytes());
        }
        self.writer.write_all(&index_bytes)?;
        let index_block_size = index_bytes.len() as u64;

        // Write the footer
        // Footer Format: [index_block_offset: u64][index_block_size: u64][magic_number: u64]
        self.writer.write_all(&index_block_offset.to_le_bytes())?;
        self.writer.write_all(&index_block_size.to_le_bytes())?;
        self.writer.write_all(&0xDEADBEEFCAFEBABEu64.to_le_bytes())?; // Magic number

        self.writer.flush()?;
        Ok(())
    }
}