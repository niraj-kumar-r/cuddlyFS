use std::sync::Arc;

use tonic::{Request, Response};

use crate::cuddlyproto::{
    node_service_server::NodeService, BlockReceivedRequest, BlockReceivedResponse, StatusCode,
    StatusEnum,
};

use super::namenode_data_registry::DataRegistry;

pub struct NamenodeNodeService {
    data_registry: Arc<DataRegistry>,
}

impl NamenodeNodeService {
    pub(super) fn new(data_registry: Arc<DataRegistry>) -> Self {
        Self { data_registry }
    }
}

#[tonic::async_trait]
impl NodeService for NamenodeNodeService {
    async fn block_received(
        &self,
        request: Request<BlockReceivedRequest>,
    ) -> Result<Response<BlockReceivedResponse>, tonic::Status> {
        let request = request.into_inner();
        let address = request.address;
        let block = request.block.unwrap_or_default();

        match self.data_registry.block_received(&address, &block) {
            Ok(()) => Ok(Response::new(BlockReceivedResponse {
                status: Some(StatusCode {
                    success: true,
                    code: StatusEnum::Ok as i32,
                    message: "Block received".to_string(),
                }),
            })),
            Err(_status) => Err(tonic::Status::invalid_argument("Invalid argument")),
        }
    }
}
