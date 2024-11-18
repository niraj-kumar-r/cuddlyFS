use crate::{
    block,
    cuddlyproto::{file_service_client::FileServiceClient, OpenFileRequest},
    errors::{CuddlyError, CuddlyResult},
};

pub struct CuddlyReader {}

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

        let block_with_locations = res.blocks_with_locations;

        Ok(Self {})
    }
}
