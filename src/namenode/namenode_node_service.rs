use std::sync::Arc;

use tonic::{Request, Response};

use super::namenode_data_registry::DataRegistry;
use crate::{
    block::Block,
    cuddlyproto::{
        node_service_server::NodeService, BlockReceivedRequest, BlockReceivedResponse,
        HeartbeatRequest, HeartbeatResponse, StatusCode, StatusEnum,
    },
};

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
        let request: BlockReceivedRequest = request.into_inner();
        let BlockReceivedRequest { address, block } = request;
        let block: Block = block.unwrap_or_default().into();
        // todo : may need something other than default

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

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, tonic::Status> {
        let request_data = request.into_inner();

        let response = self
            .data_registry
            .handle_heartbeat(request_data.registration.unwrap(), request_data.reports);

        Ok(Response::new(response))
    }
}
