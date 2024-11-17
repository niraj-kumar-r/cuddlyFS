use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
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

// #[allow(dead_code)]
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

    pub(crate) async fn get_blockfile<P: AsRef<Path>>(
        &self,
        path: P,
        create_if_missing: bool,
    ) -> CuddlyResult<File> {
        let path = self.block_directory.join(path);

        match path.try_exists() {
            Ok(true) => {}
            Ok(false) => {
                if create_if_missing {
                    // If flag is true, create the file
                    let file = File::create(&path).await.map_err(|_err| {
                        CuddlyError::IOError(format!("Failed to create block file at: {:?}", path))
                    })?;
                    return Ok(file);
                } else {
                    // If flag is false, return an error
                    return Err(CuddlyError::IOError(format!(
                        "Block file {:?} does not exist",
                        path
                    )));
                }
            }
            Err(err) => {
                return Err(CuddlyError::IOError(format!(
                    "Failed to check if Block file exist, error {:?}",
                    err
                )));
            }
        }

        let file = File::open(&path).await.map_err(|_err| {
            CuddlyError::IOError(format!("Failed to open block file at: {:?}", path))
        })?;

        Ok(file)
    }

    pub(crate) async fn start_block_creation(&self, block: &Block) -> CuddlyResult<File> {
        if self.block_exists(block) {
            return Err(CuddlyError::IOError(format!(
                "Block {:?} already exists",
                block
            )));
        }

        self.insert_in_progress_block(block)?;
        let block_path = self.block_directory.join(block.filename() + ".tmp");
        let file = match File::create(block_path).await {
            Ok(file) => file,
            Err(e) => {
                let mut blocks_being_created = self.blocks_being_created.lock().unwrap();
                blocks_being_created.deref_mut().remove(block);
                return Err(e.into());
            }
        };

        Ok(file)
    }

    pub(crate) async fn abort_block_creation(&self, block: &Block) -> CuddlyResult<()> {
        self.remove_in_progress_block(block)
    }

    pub(crate) async fn finish_block_creation(&self, block: &Block) -> CuddlyResult<()> {
        self.remove_in_progress_block(block)?;
        let filename = self.block_directory.join(block.filename());
        let temp_filename = self
            .block_directory
            .join(format!("{}.tmp", block.filename()));
        tokio::fs::rename(temp_filename, filename).await?;

        Ok(())
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
        self.block_directory
            .join(block.filename())
            .try_exists()
            .unwrap_or(false)
    }

    pub(crate) fn get_filepath_for_block_id(&self, block_id: &str) -> PathBuf {
        self.block_directory.join(block_id)
    }
}
