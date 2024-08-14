use std::{io, net};
use tokio::sync::mpsc;
mod tcp_transport;

// peer is a trait that represents the remote node
pub trait Peer {
    fn send(&self, data: &[u8]) -> io::Result<()>;
    fn close_stream(&self);
}

// Transport is anything that handles the communication between nodes
pub trait Transport {
    fn addr(&self) -> &net::SocketAddr;
    async fn dial(&self, address: &net::SocketAddr) -> io::Result<()>;
    async fn listen_and_accept(&mut self) -> io::Result<()>;
    fn consume(&self) -> mpsc::Receiver<RPC>;
    async fn close(&mut self) -> io::Result<()>;
}

// Placeholder for RPC type
pub struct RPC;
