use crate::message::Message;
use crate::node::Node;

pub trait Transport {
    fn send_message(&self, node: &Node, message: Message) -> Result<(), String>;
    fn receive_message(&self) -> Option<Message>;
    fn connect_to_node(&self, node: &Node) -> Result<(), String>;
}

pub struct TCPTransport;

impl Transport for TCPTransport {
    fn send_message(&self, node: &Node, message: Message) -> Result<(), String> {
        // TCP send logic
        Ok(())
    }

    fn receive_message(&self) -> Option<Message> {
        // TCP receive logic
        Some(Message::ChunkRequest {
            chunk_id: String::from("example"),
        })
    }

    fn connect_to_node(&self, node: &Node) -> Result<(), String> {
        // TCP connection logic
        Ok(())
    }
}
