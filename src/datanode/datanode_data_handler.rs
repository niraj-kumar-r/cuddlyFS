use std::sync::Arc;

use tokio::{io::BufStream, net::TcpStream};
use tracing::debug;

use crate::{errors::CuddlyResult, utils::parse_message};

use super::{
    cuddlyproto::{self, operation::OpCode},
    datanode_data_registry::DatanodeDataRegistry,
};

pub(crate) struct DatanodeDataHandler {
    tcp_stream: BufStream<TcpStream>,
    storage: Arc<DatanodeDataRegistry>,
    packet_size: u64,
    block_sender: tokio::sync::mpsc::Sender<cuddlyproto::Block>,
}

impl DatanodeDataHandler {
    pub(crate) fn new(
        tcp_stream: TcpStream,
        storage: Arc<DatanodeDataRegistry>,
        packet_size: u64,
        block_sender: tokio::sync::mpsc::Sender<cuddlyproto::Block>,
    ) -> Self {
        Self {
            tcp_stream: BufStream::new(tcp_stream),
            storage,
            packet_size,
            block_sender,
        }
    }

    pub(crate) async fn handle(&mut self) -> CuddlyResult<()> {
        match self.tcp_stream.get_ref().peer_addr() {
            Ok(peer_addr) => {
                debug!("Received request from {:?}", peer_addr);
            }
            Err(e) => debug!("Could not retrieve peer address, reason: {:?}", e),
        };
        let cuddlyproto::Operation { op } = parse_message(&mut self.tcp_stream).await?;

        match op {
            op if op == OpCode::WriteBlock as i32 => self.handle_write().await?,
            op if op == OpCode::ReadBlock as i32 => self.handle_read().await?,
            _ => unreachable!(),
        };

        Ok(())
    }

    async fn handle_write(&mut self) -> CuddlyResult<()> {
        todo!()
    }

    async fn handle_read(&mut self) -> CuddlyResult<()> {
        todo!()
    }
}
