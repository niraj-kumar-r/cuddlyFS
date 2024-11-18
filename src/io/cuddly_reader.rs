use crate::errors::CuddlyResult;

pub struct CuddlyReader {}

impl CuddlyReader {
    pub fn open(namenode_rpc_address: &str, file_path: impl Into<String>) -> CuddlyResult<Self> {
        Ok(Self {})
    }
}
