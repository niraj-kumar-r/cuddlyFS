use std::collections::{HashMap, HashSet};
use std::hash::Hash;

#[derive(Debug)]
struct BlockInfo<B, D> {
    block: B,
    datanode_ids: HashSet<D>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct BlockIdToDatanodeMap<K, B, D> {
    blockmap: HashMap<K, BlockInfo<B, D>>,
}

#[allow(dead_code)]
impl<K, B, D> BlockIdToDatanodeMap<K, B, D>
where
    K: Eq + Hash,
    D: Eq + Hash,
{
    pub(crate) fn new() -> Self {
        Self {
            blockmap: HashMap::new(),
        }
    }

    pub(crate) fn contains_block(&self, block_id: &K) -> bool {
        self.blockmap.contains_key(block_id)
    }

    pub(crate) fn contains_datanode_for_block(&self, block_id: &K, datanode_id: &D) -> bool {
        self.blockmap
            .get(block_id)
            .map(|info| info.datanode_ids.contains(datanode_id))
            .unwrap_or(false)
    }

    pub(crate) fn insert_block(&mut self, block_id: K, block: B) {
        self.blockmap.entry(block_id).or_insert_with(|| BlockInfo {
            block,
            datanode_ids: HashSet::new(),
        });
    }

    /// Insert a datanode id into the blockmap.
    /// Returns true if the block is not in the map, false otherwise.
    pub(crate) fn insert_datanode_for_block(
        &mut self,
        block_id: K,
        block: B,
        datanode_id: D,
    ) -> bool {
        if let Some(block_info) = self.blockmap.get_mut(&block_id) {
            block_info.datanode_ids.insert(datanode_id)
        } else {
            let mut datanode_ids = HashSet::new();
            datanode_ids.insert(datanode_id);
            self.blockmap.insert(
                block_id,
                BlockInfo {
                    block,
                    datanode_ids,
                },
            );
            true
        }
    }

    pub(crate) fn remove_datanode_for_block(&mut self, block_id: &K, datanode_id: &D) -> bool {
        if let Some(block_info) = self.blockmap.get_mut(block_id) {
            block_info.datanode_ids.remove(datanode_id)
        } else {
            false
        }
    }

    pub(crate) fn get_block(&self, block_id: &K) -> Option<&B> {
        self.blockmap.get(block_id).map(|info| &info.block)
    }

    pub(crate) fn get_datanodes_for_block(&self, block_id: &K) -> Option<&HashSet<D>> {
        self.blockmap.get(block_id).map(|info| &info.datanode_ids)
    }
}
