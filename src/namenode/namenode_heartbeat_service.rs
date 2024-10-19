use log::info;
use tonic::{Request, Response, Status};

use crate::cuddlyproto::{
    heartbeat_service_server::HeartbeatService, nnha_status_heartbeat_proto, HeartbeatRequest,
    HeartbeatResponse, NnhaStatusHeartbeatProto, StatusCode, StatusEnum,
};

pub struct NamenodeHeartbeatService {}

#[tonic::async_trait]
impl HeartbeatService for NamenodeHeartbeatService {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        info!(
            "Got a request from: {:?}",
            request.into_inner().registration.unwrap().datanode_id
        );

        let response = HeartbeatResponse {
            status: Some(StatusCode {
                success: true,
                code: StatusEnum::Ok as i32,
                message: "Ok".to_string(),
            }),
            ha_status: Some(NnhaStatusHeartbeatProto {
                state: nnha_status_heartbeat_proto::State::Active as i32,
                txid: 0,
            }),
        };

        Ok(Response::new(response))
    }
}
