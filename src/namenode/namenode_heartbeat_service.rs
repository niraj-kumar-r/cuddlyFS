use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::namenode_data_registry::DataRegistry;
use crate::cuddlyproto::{
    heartbeat_service_server::HeartbeatService, HeartbeatRequest, HeartbeatResponse,
};

pub struct NamenodeHeartbeatService {
    data_registry: Arc<DataRegistry>,
}

impl NamenodeHeartbeatService {
    pub fn new(data_registry: Arc<DataRegistry>) -> Self {
        Self { data_registry }
    }
}

#[tonic::async_trait]
impl HeartbeatService for NamenodeHeartbeatService {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request_data = request.into_inner();

        let response = self
            .data_registry
            .handle_heartbeat(request_data.registration.unwrap(), request_data.reports);

        Ok(Response::new(response))
    }
}
