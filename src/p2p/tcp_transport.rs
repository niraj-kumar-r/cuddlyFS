pub struct TcpTransport {
    listen_address: std::net::SocketAddr,
    listener: Option<tokio::net::TcpListener>,
    mu: tokio::sync::Mutex<()>,
    peers: std::collections::HashMap<std::net::SocketAddr, TcpPeer>,
}

pub struct TcpPeer {}
