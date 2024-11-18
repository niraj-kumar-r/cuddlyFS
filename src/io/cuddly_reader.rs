use crate::{
    cuddlyproto::{file_service_client::FileServiceClient, BlockWithLocations, OpenFileRequest},
    errors::CuddlyResult,
};

#[allow(dead_code)]
pub struct CuddlyReader {
    blocks_with_locations: Vec<BlockWithLocations>,
    total_file_size: u64,
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

        let blocks_with_locations = res.blocks_with_locations;

        let total_file_size = blocks_with_locations
            .iter()
            .fold(0, |acc, b| acc + b.block.as_ref().unwrap().len);

        Ok(Self {
            blocks_with_locations,
            total_file_size,
        })
    }
}
