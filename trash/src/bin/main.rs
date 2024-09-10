use cuddlyfs::p2p::{tcp_transport, Transport};
use tokio;

#[tokio::main]
async fn main() {
    env_logger::init();

    let tr = tcp_transport::TcpTransport::new("127.0.0.1:4000");
    match tr {
        Ok(mut transport) => {
            let _ = transport.listen_and_accept().await;
        }

        Err(e) => {
            log::error!("Failed to create transport: {:?}", e);
        }
    }
}
