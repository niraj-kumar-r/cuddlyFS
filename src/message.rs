use crate::chunk::Chunk;
use crate::metadata::ChangeProposal;
use crate::metadata::FileMetadata;
use bytes::Bytes;

pub enum Message {
    ChunkRequest { chunk_id: String },
    ChunkResponse { chunk: Chunk },
    MetadataUpdate { metadata: FileMetadata },
    Proposal { proposal: ChangeProposal },
    FileData { data: Bytes }, // Added a message type to handle raw file data
}
