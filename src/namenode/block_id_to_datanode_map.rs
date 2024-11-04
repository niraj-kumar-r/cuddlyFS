use std::collections::{HashMap, HashSet};

use crate::block::Block;

#[derive(Debug)]
struct BlockInfo {
    block: Block,
    datanode_ids: HashSet<String>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct BlockIdToDatanodeMap {
    blockmap: HashMap<u64, BlockInfo>,
}

#[allow(dead_code)]
impl BlockIdToDatanodeMap {
    pub(crate) fn new() -> Self {
        Self {
            blockmap: HashMap::new(),
        }
    }

    pub(crate) fn contains_block(&self, block_id: u64) -> bool {
        self.blockmap.contains_key(&block_id)
    }

    pub(crate) fn contains_datanode_for_block(&self, block_id: u64, datanode_id: &str) -> bool {
        self.blockmap
            .get(&block_id)
            .map(|info| info.datanode_ids.contains(datanode_id))
            .unwrap_or(false)
    }

    pub(crate) fn insert_block(&mut self, block: Block) {
        self.blockmap.entry(block.id).or_insert_with(|| BlockInfo {
            block,
            datanode_ids: HashSet::new(),
        });
    }

    /// Insert a datanode id into the blockmap.
    /// Returns true if the block is not in the map, false otherwise.
    pub(crate) fn insert_datanode_for_block(&mut self, block: &Block, datanode_id: String) -> bool {
        if let Some(block_info) = self.blockmap.get_mut(&block.id) {
            block_info.datanode_ids.insert(datanode_id)
        } else {
            let mut datanode_ids = HashSet::new();
            datanode_ids.insert(datanode_id);
            self.blockmap.insert(
                block.id,
                BlockInfo {
                    block: block.clone(),
                    datanode_ids,
                },
            );
            true
        }
    }

    pub(crate) fn remove_datanode_for_block(&mut self, block: &Block, datanode_id: &str) -> bool {
        if let Some(block_info) = self.blockmap.get_mut(&block.id) {
            block_info.datanode_ids.remove(datanode_id)
        } else {
            false
        }
    }

    pub(crate) fn get_block(&self, block_id: u64) -> Option<Block> {
        self.blockmap.get(&block_id).map(|info| info.block.clone())
    }

    pub(crate) fn get_datanodes_for_block(&self, block_id: u64) -> Option<&HashSet<String>> {
        self.blockmap.get(&block_id).map(|info| &info.datanode_ids)
    }
}
