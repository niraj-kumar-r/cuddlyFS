use tonic::Streaming;

use self::cuddlyproto::{Packet, WriteBlockResponse};

use super::{
    cuddlyproto::{self},
    datanode_data_registry::DatanodeDataRegistry,
};

pub struct DatanodeDatanodeService {}

impl DatanodeDatanodeService {
    pub fn new() -> Self {
        Self {}
    }
}

#[tonic::async_trait]
impl cuddlyproto::datanode_datanode_service_server::DatanodeDatanodeService
    for DatanodeDatanodeService
{
    async fn replicate_datanode_block(
        &self,
        request: tonic::Request<Streaming<Packet>>,
    ) -> Result<tonic::Response<WriteBlockResponse>, tonic::Status> {
        todo!()
    }
}
