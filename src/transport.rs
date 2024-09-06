use crate::message::Message;
use crate::node::Node;
use bytes::Bytes;

pub trait Transport {
    fn send_message(&self, node: &Node, message: Message) -> Result<(), String>;
    fn receive_message(&self) -> Option<Message>;
    fn send_data(&self, node: &Node, data: Bytes) -> Result<(), String>;
}

pub struct TCPTransport;

impl Transport for TCPTransport {
    fn send_message(&self, node: &Node, message: Message) -> Result<(), String> {
        // Serialization and sending logic here
        Ok(())
    }

    fn receive_message(&self) -> Option<Message> {
        // Deserialization and receiving logic here
        Some(Message::ChunkRequest {
            chunk_id: String::from("example_chunk"),
        })
    }

    fn send_data(&self, node: &Node, data: Bytes) -> Result<(), String> {
        // Send raw file data as Bytes over TCP
        Ok(())
    }
}
