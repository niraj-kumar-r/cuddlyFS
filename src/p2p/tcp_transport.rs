pub struct TcpTransport {
    listen_address: std::net::SocketAddr,
    listener: Option<tokio::net::TcpListener>,
    mu: tokio::sync::Mutex<()>,
    peers: std::collections::HashMap<std::net::SocketAddr, TcpPeer>,
}

impl TcpTransport {
    pub fn new(listen_address: &str) -> Result<Self, std::net::AddrParseError> {
        let listen_address = listen_address.parse::<std::net::SocketAddr>();
        match listen_address {
            Ok(listen_address) => Ok(Self {
                listen_address,
                listener: None,
                mu: tokio::sync::Mutex::new(()),
                peers: std::collections::HashMap::new(),
            }),
            Err(e) => {
                eprintln!("Failed to parse listen address: {:?}", e);
                Err(e)
            }
        }
    }

    pub async fn listen_and_accept(&mut self) {
        let listener = tokio::net::TcpListener::bind(&self.listen_address)
            .await
            .unwrap();
        self.listener = Some(listener);

        while let Some(listener) = &self.listener {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    println!("Accepted connection from {:?}", addr);
                    let peer = TcpPeer::new(socket, addr);
                    self.peers.insert(addr, peer);
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {:?}", e);
                }
            }
        }
    }
}

pub struct TcpPeer {
    socket: tokio::net::TcpStream,
    peer_addr: std::net::SocketAddr,
}

impl TcpPeer {
    pub fn new(socket: tokio::net::TcpStream, peer_addr: std::net::SocketAddr) -> Self {
        Self { socket, peer_addr }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tcp_transport() {
        let transport = TcpTransport::new("127.0.0.1:4000");
        assert!(transport.is_ok());

        // test the address
        let transport = transport.unwrap();
        assert_eq!(transport.listen_address, "127.0.0.1:4000".parse().unwrap());
    }
}
