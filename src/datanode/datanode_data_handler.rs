use std::sync::Arc;

use log::debug;
use log::info;
use log::warn;
use prost::Message;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::{
    io::{BufReader, BufStream},
    net::TcpStream,
};

use crate::block::Block;
use crate::{errors::CuddlyResult, utils::parse_message};

use self::cuddlyproto::WriteBlockOperation;
use self::cuddlyproto::WriteBlockResponse;

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
        info!("Handling data server request");
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
        let WriteBlockOperation { block, targets } =
            parse_message::<WriteBlockOperation>(&mut self.stream).await?;
        let block: Block = block.unwrap().into();
        let block_file = self.data_registry.start_block_creation(&block).await?;

        match self.write_block(block_file, &block, &targets[1..]).await {
            Ok(()) => {
                self.data_registry.finish_block_creation(&block).await?;
                let response = WriteBlockResponse { success: true };
                let mut buffer = vec![];
                response.encode_length_delimited(&mut buffer)?;
                self.stream.write_all(&buffer).await?;
                self.stream.flush().await?;
                Ok(())
            }
            Err(e) => {
                self.data_registry.abort_block_creation(&block).await?;
                Err(e)
            }
        }
    }

    async fn write_block(
        &mut self,
        block_file: fs::File,
        block: &Block,
        targets: &[String],
    ) -> CuddlyResult<()> {
        let mut block_file = BufWriter::new(block_file);

        let mut buffer = vec![];
        let mut next_node = if !targets.is_empty() {
            let address = &targets[0];
            let mut stream = BufStream::new(TcpStream::connect(address).await?);

            cuddlyproto::Operation {
                op: OpCode::WriteBlock as i32,
            }
            .encode_length_delimited(&mut buffer)?;
            stream.write_all(&buffer).await?;
            buffer.clear();

            WriteBlockOperation {
                block: Some(block.clone().into()),
                targets: targets.into(),
            }
            .encode_length_delimited(&mut buffer)?;
            stream.write_all(&buffer).await?;
            buffer.clear();

            Some(stream)
        } else {
            None
        };

        let mut total_block_length: u64 = 0;
        loop {
            let Packet { size, last } = parse_message::<Packet>(&mut self.stream).await?;
            buffer.resize_with(size as usize, u8::default);
            total_block_length += self.stream.read_exact(&mut buffer).await? as u64;
            block_file.write_all(&buffer).await?;

            if let Some(ref mut stream) = next_node {
                let packet = Packet { size, last };
                let mut message_buffer = vec![];
                packet.encode_length_delimited(&mut message_buffer)?;
                stream.write_all(&message_buffer).await?;
                stream.write_all(&buffer).await?;
            }

            buffer.clear();
            if last {
                break;
            }
        }

        if let Some(ref mut stream) = next_node {
            stream.flush().await?;
        }

        block_file.flush().await?;

        if let Some(ref mut stream) = next_node {
            let WriteBlockResponse { success } =
                parse_message::<WriteBlockResponse>(stream).await?;
            if !success {
                debug!("Unexpected failure from {}", targets[0]);
            }
        }

        let block = cuddlyproto::Block {
            id: block.id.to_string(),
            len: total_block_length,
            seq: block.seq,
        };

        if self.block_sender.send(block.clone()).await.is_err() {
            warn!(
                "Receiver dropped. Namenode will not be informed of new written block {:?}",
                block
            );
        }

        Ok(())
    }
}
