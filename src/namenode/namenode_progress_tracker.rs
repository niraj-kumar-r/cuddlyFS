use std::collections::HashMap;

use uuid::Uuid;

use crate::errors::{CuddlyError, CuddlyResult};

pub(crate) struct NamenodeProgressTracker {
    filename_to_blocks: HashMap<String, Vec<Uuid>>,
    block_to_replication_count: HashMap<Uuid, u64>,
}

#[allow(dead_code)]
impl NamenodeProgressTracker {
    pub(crate) fn new() -> Self {
        Self {
            filename_to_blocks: HashMap::new(),
            block_to_replication_count: HashMap::new(),
        }
    }

    pub(crate) fn block_ids(&self, filename: &str) -> CuddlyResult<&[Uuid]> {
        let blocks = self.filename_to_blocks.get(filename);
        if let Some(blocks) = blocks {
            Ok(blocks)
        } else {
            Err(CuddlyError::FSError(format!(
                "'{}': File creation has not started yet",
                filename
            )))
        }
    }

    pub(crate) fn replication_count(&self, block_id: Uuid) -> u64 {
        *self.block_to_replication_count.get(&block_id).unwrap_or(&0)
    }

    pub(crate) fn increment_replication(&mut self, block_id: Uuid) {
        let prev_count = *self.block_to_replication_count.get(&block_id).unwrap_or(&0);
        self.block_to_replication_count
            .insert(block_id, prev_count + 1);
    }

    pub(crate) fn add_file(&mut self, filename: String) -> CuddlyResult<()> {
        if self.filename_to_blocks.contains_key(&filename) {
            return Err(CuddlyError::FSError(format!(
                "'{}': File creation already in progress",
                filename
            )));
        }
        self.filename_to_blocks.insert(filename, Vec::new());
        Ok(())
    }

    pub(crate) fn remove_file(&mut self, filename: &str) -> CuddlyResult<()> {
        let blocks = self.filename_to_blocks.remove(filename);
        if let Some(blocks) = blocks {
            for block_id in blocks {
                self.block_to_replication_count.remove(&block_id);
            }
            Ok(())
        } else {
            Err(CuddlyError::FSError(format!(
                "'{}': File creation has not started yet",
                filename
            )))
        }
    }

    pub(crate) fn add_block(&mut self, filename: &str, block_id: Uuid) -> CuddlyResult<()> {
        let blocks = self.filename_to_blocks.get_mut(filename);
        if let Some(blocks) = blocks {
            blocks.push(block_id);
            self.block_to_replication_count.insert(block_id, 0);
            Ok(())
        } else {
            Err(CuddlyError::FSError(format!(
                "'{}': File creation has not started yet",
                filename
            )))
        }
    }

    pub(crate) fn contains_block(&self, block_id: Uuid) -> bool {
        self.block_to_replication_count.contains_key(&block_id)
    }

    pub(crate) fn remove_block(&mut self, filename: &str, block_id: Uuid) -> CuddlyResult<()> {
        let blocks = self.filename_to_blocks.get_mut(filename);
        if let Some(blocks) = blocks {
            blocks.retain(|&id| id != block_id);
            Ok(())
        } else {
            Err(CuddlyError::FSError(format!(
                "'{}': File creation has not started yet",
                filename
            )))
        }
    }
}
