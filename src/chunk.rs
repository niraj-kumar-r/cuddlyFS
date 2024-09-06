pub struct Chunk {
    pub chunk_id: String,
    pub data: Vec<u8>,
    pub checksum: String,
}

impl Chunk {
    pub fn new(chunk_id: String, data: Vec<u8>) -> Self {
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

fn calculate_checksum(data: &[u8]) -> String {
    format!("{:x}", md5::compute(data))
}
