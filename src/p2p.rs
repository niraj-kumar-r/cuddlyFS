pub mod tcp_transport;

pub trait Transport {
    async fn listen_and_accept(&mut self) -> Result<(), std::io::Error>;
}
