use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::cuddlyproto;

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct Block {
    pub id: Uuid,
    pub len: u64,
    pub seq: u64,
}

impl Block {
    pub fn new(id: Uuid, len: u64, seq: u64) -> Self {
        Self { id, len, seq }
    }

    pub fn filename(&self) -> String {
        format!("block_{}", self.id)
    }
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
        let cuddlyproto::Block { id, len, seq } = value;
        let id = Uuid::parse_str(&id).unwrap();
        Self { id, len, seq }
    }
}

impl From<Block> for cuddlyproto::Block {
    fn from(value: Block) -> Self {
        let Block { id, len, seq } = value;
        let id = id.to_string();
        Self { id, len, seq }
    }
}
