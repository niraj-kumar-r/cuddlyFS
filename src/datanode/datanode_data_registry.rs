use std::{collections::HashSet, path::PathBuf, sync::Mutex};

use crate::{block::Block, errors::CuddlyResult};

use super::datanode_disk_info::DiskInfo;

#[derive(Debug)]
pub(crate) struct DatanodeDataRegistry {
    disk_info: Mutex<DiskInfo>,
    blocks_being_created: Mutex<HashSet<Block>>,
    block_directory: PathBuf,
}

impl DatanodeDataRegistry {
    pub(crate) fn new(data_dir: &PathBuf) -> CuddlyResult<Self> {
        let disk_info = Mutex::new(DiskInfo::new(data_dir)?);
        let block_directory = data_dir.clone().join("blocks");
        Ok(Self {
            disk_info,
            blocks_being_created: Mutex::new(HashSet::new()),
            block_directory,
        })
    }
}
