pub mod tcp_transport;

pub trait Transport {
    fn listen_and_accept(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), std::io::Error>> + Send;
}
