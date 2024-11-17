use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::Mutex,
};

use tokio::fs::File;

use crate::{
    block::Block,
    errors::{CuddlyError, CuddlyResult},
};

use super::datanode_disk_info::DiskInfo;

#[derive(Debug)]
pub(crate) struct DatanodeDataRegistry {
    disk_info: Mutex<DiskInfo>,
    blocks_being_created: Mutex<HashSet<Block>>,
    block_directory: PathBuf,
}

#[allow(dead_code)]
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

    pub(crate) fn used(&self) -> CuddlyResult<u64> {
        self.disk_info.lock().unwrap().get_used()
    }

    pub(crate) fn available(&self) -> CuddlyResult<u64> {
        self.disk_info.lock().unwrap().get_available()
    }

    pub(crate) fn get_blockfile(&self, block: &Block) -> CuddlyResult<File> {
        let path = self.block_directory.join(block.filename());
        let file = std::fs::File::open(path)?;
        Ok(File::from_std(file))
    }

    fn insert_in_progress_block(&self, block: &Block) -> CuddlyResult<()> {
        let mut blocks_being_created = self.blocks_being_created.lock().unwrap();
        if blocks_being_created.deref().contains(block) {
            return Err(CuddlyError::IOError(format!(
                "A creation of block {:?} is already in progress",
                block
            )));
        }
        blocks_being_created.deref_mut().insert(*block);
        Ok(())
    }

    fn remove_in_progress_block(&self, block: &Block) -> CuddlyResult<()> {
        let mut blocks_being_created = self.blocks_being_created.lock().unwrap();
        if !blocks_being_created.deref().contains(block) {
            return Err(CuddlyError::IOError(format!(
                "Block creation for {:?} has not been initiated",
                block
            )));
        }
        blocks_being_created.deref_mut().remove(block);
        Ok(())
    }

    fn block_exists(&self, block: &Block) -> bool {
        self.block_directory.join(block.filename()).exists()
    }
}
