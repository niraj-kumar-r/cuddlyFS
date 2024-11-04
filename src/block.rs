use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use crate::cuddlyproto;

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct Block {
    pub id: u64,
    pub len: u64,
}

impl Block {
    pub fn new(id: u64, len: u64) -> Self {
        Self { id, len }
    }

    // pub fn filename(&self) -> String {
    //     format!("block_{}", self.id)
    // }
}

impl std::fmt::Display for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Block {{ id: {}, len: {} }}", self.id, self.len)
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Block {}

impl Hash for Block {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl From<cuddlyproto::Block> for Block {
    fn from(value: cuddlyproto::Block) -> Self {
        let cuddlyproto::Block { id, len } = value;
        Self { id, len }
    }
}

impl From<Block> for cuddlyproto::Block {
    fn from(value: Block) -> Self {
        let Block { id, len } = value;
        Self { id, len }
    }
}
