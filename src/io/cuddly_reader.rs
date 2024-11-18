use tokio::io::AsyncRead;

use crate::{
    cuddlyproto::{file_service_client::FileServiceClient, BlockWithLocations, OpenFileRequest},
    errors::CuddlyResult,
};

#[allow(dead_code)]
pub struct CuddlyReader {
    blocks_with_locations: Vec<BlockWithLocations>,
    total_file_size: u64,
    current_block_seq: u64,
    current_block_offset: u64,
}

impl CuddlyReader {
    pub async fn open(
        namenode_rpc_address: String,
        file_path: impl Into<String>,
    ) -> CuddlyResult<Self> {
        let mut namenode_client =
            match FileServiceClient::connect(namenode_rpc_address.clone()).await {
                Ok(client) => client,
                Err(e) => {
                    return Err(e.into());
                }
            };

        let res = namenode_client
            .open_file(OpenFileRequest {
                auth_token: None,
                file_path: file_path.into(),
            })
            .await?
            .into_inner();

        let mut blocks_with_locations = res.blocks_with_locations;

        let total_file_size = blocks_with_locations
            .iter()
            .fold(0, |acc, b| acc + b.block.as_ref().unwrap().len);

        blocks_with_locations.sort_by(|a, b| {
            a.block
                .as_ref()
                .unwrap()
                .seq
                .cmp(&b.block.as_ref().unwrap().seq)
        });

        let current_block_seq = blocks_with_locations[0].block.as_ref().unwrap().seq;

        Ok(Self {
            blocks_with_locations,
            total_file_size,
            current_block_seq,
            current_block_offset: 0,
        })
    }
}

impl AsyncRead for CuddlyReader {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        todo!()
    }
}
