use std::collections::HashMap;

use uuid::Uuid;

use crate::errors::{CuddlyError, CuddlyResult};

#[derive(Debug)]
pub(crate) struct NamenodeProgressTracker {
    filename_to_blocks: HashMap<String, Vec<Uuid>>,
    block_to_replication_count: HashMap<Uuid, u64>,
}

impl NamenodeProgressTracker {
    pub(crate) fn new() -> Self {
        Self {
            filename_to_blocks: HashMap::new(),
            block_to_replication_count: HashMap::new(),
        }
    }

    pub(crate) fn get_block_ids(&self, filename: &str) -> CuddlyResult<&[Uuid]> {
        if let Some(blocks) = self.filename_to_blocks.get(filename) {
            Ok(blocks)
        } else {
            Err(CuddlyError::FSError(format!(
                "'{}': File creation has not started yet",
                filename
            )))
        }
    }

    /// Returns the replication count for a given block ID.
    pub(crate) fn get_replication_count(&self, block_id: Uuid) -> u64 {
        *self.block_to_replication_count.get(&block_id).unwrap_or(&0)
    }
    /// Increments the replication count for the given block ID.
    /// Increments the replication count for the given block ID.
    pub(crate) fn increment_replication(&mut self, block_id: Uuid) {
        let prev_count = *self.block_to_replication_count.get(&block_id).unwrap_or(&0);
        self.block_to_replication_count
            .insert(block_id, prev_count + 1);
    }
    /// Adds a new file to the tracker. Returns an error if the file already exists.
    pub(crate) fn add_file(&mut self, filename: String) -> CuddlyResult<()> {
        if self.filename_to_blocks.contains_key(&filename) {
            return Err(CuddlyError::FSError(format!(
                "'{}': File creation already in progress",
                filename
            )));
        }
        self.filename_to_blocks.insert(filename, Vec::new());
        // self.block_to_replication_count.remove(&block_id);
        Ok(())
    }

    /// Removes a file and its associated blocks from the tracker.
    pub(crate) fn remove_file(&mut self, filename: &str) -> CuddlyResult<()> {
        if let Some(blocks) = self.filename_to_blocks.remove(filename) {
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

    /// Checks if the given block ID is being tracked.
    pub(crate) fn contains_block(&self, block_id: &Uuid) -> bool {
        self.block_to_replication_count.contains_key(block_id)
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

    /// Removes a block from the specified file. Returns an error if the file does not exist.
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
