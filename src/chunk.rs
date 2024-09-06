use bytes::Bytes;

pub struct Chunk {
    pub chunk_id: String,
    pub data: Bytes,      // Efficient zero-copy byte data
    pub checksum: String, // Integrity verification
}

impl Chunk {
    pub fn new(chunk_id: String, data: Bytes) -> Self {
        let checksum = calculate_checksum(&data);
        Chunk {
            chunk_id,
            data,
            checksum,
        }
    }

    pub fn verify_checksum(&self) -> bool {
        self.checksum == calculate_checksum(&self.data)
    }
}

// Updated checksum calculation to accept Bytes
fn calculate_checksum(data: &Bytes) -> String {
    format!("{:x}", md5::compute(data)) // MD5 checksum as an example
}
