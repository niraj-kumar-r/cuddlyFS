pub struct FileMetadata {
    pub file_path: String,
    pub size: u64,
    pub version: u32,
    pub chunks: Vec<String>,
}

pub trait MetadataManager {
    fn get_file_metadata(&self, file_path: &str) -> Option<FileMetadata>;
    fn save_file_metadata(&mut self, metadata: FileMetadata) -> Result<(), String>;
}

pub struct ChangeProposal {
    pub file_path: String,
    pub operation: Operation,
    pub version: u32,
    pub timestamp: u64,
}

pub enum Operation {
    AddFile,
    DeleteFile,
    UpdateFile,
}
