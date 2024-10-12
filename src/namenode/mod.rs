use std::net::SocketAddr;

use crate::cuddlyproto::{
    heartbeat_service_server::{HeartbeatService, HeartbeatServiceServer},
    nnha_status_heartbeat_proto, HeartbeatRequest, HeartbeatResponse, NnhaStatusHeartbeatProto,
    StatusCode, StatusEnum,
};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Debug, Default, Clone)]
pub struct Namenode {
    cancel_token: CancellationToken,
}

impl Namenode {
    pub fn new(cancel_token: CancellationToken) -> Self {
        Self { cancel_token }
    }

    pub async fn run(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        Server::builder()
            .add_service(HeartbeatServiceServer::new(Namenode::clone(&self)))
            .serve(addr)
            .await?;

        Ok(())
    }
}

#[tonic::async_trait]
impl HeartbeatService for Namenode {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        println!("Got a request: {:?}", request);

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
