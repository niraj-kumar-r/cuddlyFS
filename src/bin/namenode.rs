use std::net::SocketAddr;
use tonic::{transport::Server, Request, Response, Status};

use cuddlyfs::cuddlyproto::heartbeat_service_server::{HeartbeatService, HeartbeatServiceServer};
use cuddlyfs::cuddlyproto::{
    nnha_status_heartbeat_proto, HeartbeatRequest, HeartbeatResponse, NnhaStatusHeartbeatProto,
    StatusCode, StatusEnum,
};

#[derive(Debug, Default)]
pub struct Namenode {}

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "[::1]:50051".parse().unwrap();
    let namenode: Namenode = Namenode::default();

    Server::builder()
        .add_service(HeartbeatServiceServer::new(namenode))
        .serve(addr)
        .await?;

    Ok(())
}
