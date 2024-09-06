use crate::chunk::Chunk;
use crate::metadata::ChangeProposal;
use crate::metadata::FileMetadata;

pub enum Message {
    ChunkRequest { chunk_id: String },
    ChunkResponse { chunk: Chunk },
    MetadataUpdate { metadata: FileMetadata },
    Proposal { proposal: ChangeProposal },
}
