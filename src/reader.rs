use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

// A deserialized representation of an index entry
#[derive(Debug)]
struct IndexEntryInfo {
    last_key: Vec<u8>,
    block_offset: u64,
    block_size: u64,
}

/// Reads from an SST file.
pub struct SstReader {
    file: File,
    index: Vec<IndexEntryInfo>,
}

impl SstReader {
    /// Opens an SST file and loads its index.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;

        // Read footer to find the index
        file.seek(SeekFrom::End(-24))?; // Footer is 3 * 8 bytes
        let mut footer_buf = [0u8; 24];
        file.read_exact(&mut footer_buf)?;

        let magic = u64::from_le_bytes(footer_buf[16..24].try_into().unwrap());
        if magic != 0xDEADBEEFCAFEBABEu64 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid SST file format"));
        }

        let index_offset = u64::from_le_bytes(footer_buf[0..8].try_into().unwrap());
        let index_size = u64::from_le_bytes(footer_buf[8..16].try_into().unwrap());

        // Read and parse the index block
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_buf = vec![0; index_size as usize];
        file.read_exact(&mut index_buf)?;
        
        let index = Self::parse_index(&index_buf)?;

        Ok(SstReader { file, index })
    }

    fn parse_index(mut buf: &[u8]) -> io::Result<Vec<IndexEntryInfo>> {
        let num_entries = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        buf = &buf[4..];
        
        let mut index = Vec::with_capacity(num_entries as usize);
        for _ in 0..num_entries {
            let key_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            buf = &buf[4..];
            let last_key = buf[..key_len].to_vec();
            buf = &buf[key_len..];

            let block_offset = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            buf = &buf[8..];
            let block_size = u64::from_le_bytes(buf[0..8].try_into().unwrap());
            buf = &buf[8..];
            
            index.push(IndexEntryInfo { last_key, block_offset, block_size });
        }
        Ok(index)
    }

    /// Searches for a key and returns the corresponding value.
    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // Find the data block that might contain the key
        // The first block whose last_key is >= our key is the one to search
        let block_info = self.index.iter().find(|entry| &entry.last_key[..] >= key);
        
        if let Some(info) = block_info {
            // Read the data block from the file
            self.file.seek(SeekFrom::Start(info.block_offset))?;
            let mut block_buf = vec![0; info.block_size as usize];
            self.file.read_exact(&mut block_buf)?;
            
            // Search within the block
            return Self::search_in_block(&block_buf, key);
        }

        Ok(None)
    }

    // Linear scan through the data block to find the key
    fn search_in_block(mut buf: &[u8], search_key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let num_entries = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        buf = &buf[4..];

        for _ in 0..num_entries {
            let key_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            buf = &buf[4..];
            let key = &buf[..key_len];
            buf = &buf[key_len..];
            
            let val_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
            buf = &buf[4..];
            
            if key == search_key {
                return Ok(Some(buf[..val_len].to_vec()));
            }
            
            buf = &buf[val_len..];
        }

        Ok(None)
    }
}