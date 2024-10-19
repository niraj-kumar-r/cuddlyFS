use std::net::SocketAddr;

use crate::cuddlyproto::{
    heartbeat_service_server::{HeartbeatService, HeartbeatServiceServer},
    nnha_status_heartbeat_proto, HeartbeatRequest, HeartbeatResponse, NnhaStatusHeartbeatProto,
    StatusCode, StatusEnum,
};
use log::info;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};

#[derive(Debug, Clone)]
pub struct Namenode {
    cancel_token: CancellationToken,
    shutdown_send: mpsc::UnboundedSender<i8>,
}

impl Namenode {
    pub fn new(cancel_token: CancellationToken, shutdown_send: UnboundedSender<i8>) -> Self {
        Self {
            cancel_token,
            shutdown_send,
        }
    }

    pub async fn run(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        tokio::select! {
        _ = self.cancel_token.cancelled() => {
            info!("Namenode Run cancelled");
        },
        _ = Server::builder()
        .add_service(HeartbeatServiceServer::new(NamenodeHeartbeatService {}))
        .serve(addr) => {}
        }

        Ok(())
    }
}

pub struct NamenodeHeartbeatService {}

#[tonic::async_trait]
impl HeartbeatService for NamenodeHeartbeatService {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        info!(
            "Got a request from: {:?}",
            request
                .into_inner()
                .registration
                .unwrap()
                .datanode_id
                .unwrap()
                .datanode_uuid
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
