use std::sync::Arc;

use prost::Message;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::{
    io::{BufReader, BufStream},
    net::TcpStream,
};
use tracing::debug;

use crate::block::Block;
use crate::{errors::CuddlyResult, utils::parse_message};

use super::{
    cuddlyproto::{self, operation::OpCode, Packet, ReadBlockOperation},
    datanode_data_registry::DatanodeDataRegistry,
};

pub(crate) struct DatanodeDataHandler {
    stream: BufStream<TcpStream>,
    data_registry: Arc<DatanodeDataRegistry>,
    packet_size: u64,
    block_sender: tokio::sync::mpsc::Sender<cuddlyproto::Block>,
}

impl DatanodeDataHandler {
    pub(crate) fn new(
        stream: TcpStream,
        data_registry: Arc<DatanodeDataRegistry>,
        packet_size: u64,
        block_sender: tokio::sync::mpsc::Sender<cuddlyproto::Block>,
    ) -> Self {
        Self {
            stream: BufStream::new(stream),
            data_registry,
            packet_size,
            block_sender,
        }
    }

    pub(crate) async fn handle(&mut self) -> CuddlyResult<()> {
        match self.stream.get_ref().peer_addr() {
            Ok(peer_addr) => {
                debug!("Received request from {:?}", peer_addr);
            }
            Err(e) => debug!("Could not retrieve peer address, reason: {:?}", e),
        };
        let cuddlyproto::Operation { op } = parse_message(&mut self.stream).await?;

        match op {
            op if op == OpCode::WriteBlock as i32 => self.handle_write().await?,
            op if op == OpCode::ReadBlock as i32 => self.handle_read().await?,
            _ => unreachable!(),
        };

        Ok(())
    }

    async fn handle_read(&mut self) -> CuddlyResult<()> {
        let ReadBlockOperation { block } =
            parse_message::<ReadBlockOperation>(&mut self.stream).await?;
        let block: Block = block.unwrap().into();
        let mut buffer = vec![];
        let mut blockfile = BufReader::new(
            self.data_registry
                .get_blockfile(&block.filename(), false)
                .await?,
        );
        let mut remaining_to_send = blockfile.get_ref().metadata().await?.len();

        while remaining_to_send > 0 {
            let packet_size = std::cmp::min(remaining_to_send, self.packet_size);
            remaining_to_send -= packet_size;
            let packet = Packet {
                size: packet_size,
                last: remaining_to_send == 0,
            };
            packet.encode_length_delimited(&mut buffer)?;
            self.stream.write_all(&buffer).await?;
            buffer.clear();

            buffer.resize_with(packet_size as usize, u8::default);
            blockfile.read_exact(&mut buffer).await?;
            self.stream.write_all(&buffer).await?;
            buffer.clear();
        }

        self.stream.flush().await?;
        Ok(())
    }

    async fn handle_write(&mut self) -> CuddlyResult<()> {
        todo!()
    }
}
