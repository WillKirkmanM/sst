use std::path::Path;

use crate::{reader::SstReader, writer::SstWriter};

pub mod reader;
pub mod writer;

fn main() -> std::io::Result<()> {
    let sst_path = Path::new("example.sst");

    // === Writing the SST file ===
    println!("Writing SST file...");
    let mut writer = SstWriter::new(sst_path)?;
    
    // Add data in sorted order
    writer.add(b"apple", b"A fruit that grows on trees.")?;
    writer.add(b"banana", b"An elongated, edible fruit.")?;
    writer.add(b"cherry", b"A small, round stone fruit.")?;
    writer.add(b"date", b"A sweet, dark brown oval fruit.")?;
    writer.add(b"elderberry", b"A dark purple berry.")?;
    
    writer.finish()?;
    println!("SST file 'example.sst' created.");

    // === Reading from the SST file ===
    println!("\nReading from SST file...");
    let mut reader = SstReader::open(sst_path)?;

    // --- Test Case 1: Key exists ---
    let key_to_find = b"cherry";
    match reader.get(key_to_find)? {
        Some(value) => {
            println!(
                "Found key '{}': '{}'",
                String::from_utf8_lossy(key_to_find),
                String::from_utf8_lossy(&value)
            );
        }
        None => println!("Key '{}' not found.", String::from_utf8_lossy(key_to_find)),
    }
    
    // --- Test Case 2: Key does not exist ---
    let key_to_find_2 = b"fig";
     match reader.get(key_to_find_2)? {
        Some(value) => {
             println!(
                "Found key '{}': '{}'",
                String::from_utf8_lossy(key_to_find_2),
                String::from_utf8_lossy(&value)
            );
        }
        None => println!("Key '{}' not found.", String::from_utf8_lossy(key_to_find_2)),
    }
    
     // --- Test Case 3: First key ---
    let key_to_find_3 = b"apple";
     match reader.get(key_to_find_3)? {
        Some(value) => {
             println!(
                "Found key '{}': '{}'",
                String::from_utf8_lossy(key_to_find_3),
                String::from_utf8_lossy(&value)
            );
        }
        None => println!("Key '{}' not found.", String::from_utf8_lossy(key_to_find_3)),
    }

    Ok(())
}