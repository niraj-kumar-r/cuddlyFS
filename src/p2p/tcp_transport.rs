use log;

use super::Transport;
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
                log::error!("Failed to parse listen address: {:?}", e);
                Err(e)
            }
        }
    }

    async fn start_accept_loop(&mut self) -> Result<(), std::io::Error> {
        while let Some(listener) = &self.listener {
            match listener.accept().await {
                Ok((socket, addr)) => {
                    let peer = TcpPeer::new(socket, addr, true);
                    self.peers.insert(addr, peer);
                    log::info!("Accepted connection from {:?}", addr);
                    log::info!("Peers: {:?}", self.peers);
                }
                Err(e) => {
                    log::warn!("Failed to accept connection: {:?}", e);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

impl Transport for TcpTransport {
    async fn listen_and_accept(&mut self) -> Result<(), std::io::Error> {
        let listener = tokio::net::TcpListener::bind(&self.listen_address).await;

        match listener {
            Ok(listener) => {
                log::info!("Listening on {}", self.listen_address);
                self.listener = Some(listener);
            }
            Err(e) => {
                log::warn!("Failed to bind listener: {:?}", e);
                return Err(e);
            }
        }

        self.start_accept_loop().await
    }
}

#[derive(Debug)]
pub struct TcpPeer {
    socket: tokio::net::TcpStream,
    peer_addr: std::net::SocketAddr,
    is_outbound: bool,
    // if we dial and retrieve a conn => outbound == true
    // if we accept and retrieve a conn => outbound == false
}

impl TcpPeer {
    pub fn new(
        socket: tokio::net::TcpStream,
        peer_addr: std::net::SocketAddr,
        is_outbound: bool,
    ) -> Self {
        Self {
            socket,
            peer_addr,
            is_outbound,
        }
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

        // test the listener
        assert!(transport.listener.is_none());
    }
}
