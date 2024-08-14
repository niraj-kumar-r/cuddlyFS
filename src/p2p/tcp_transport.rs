use std::io;
use std::net;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use super::Transport;
use super::RPC;

pub struct TCPPeer {
    stream: TcpStream,
    outbound: bool,
}

impl TCPPeer {
    pub fn new(stream: TcpStream, outbound: bool) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(TCPPeer { stream, outbound }))
    }

    pub async fn send(&mut self, data: &[u8]) -> io::Result<()> {
        self.stream.write_all(data).await?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct TCPTransportOpts {
    pub listen_addr: net::SocketAddr,
    pub handshake_func: fn(Arc<Mutex<TCPPeer>>) -> io::Result<()>,
    pub on_peer: Option<fn(Arc<Mutex<TCPPeer>>) -> io::Result<()>>,
}

pub struct TCPTransport {
    opts: TCPTransportOpts,
    listener: Option<TcpListener>,
    rpcch: mpsc::Sender<RPC>,
}

impl TCPTransport {
    pub fn new(opts: TCPTransportOpts) -> Self {
        let (sender, _) = mpsc::channel(1024);
        TCPTransport {
            opts,
            listener: None,
            rpcch: sender,
        }
    }
}

impl Transport for TCPTransport {
    fn addr(&self) -> &net::SocketAddr {
        &self.opts.listen_addr
    }
    fn consume(&self) -> mpsc::Receiver<RPC> {
        let (_, receiver) = mpsc::channel(1024);
        receiver
    }

    async fn close(&mut self) -> io::Result<()> {
        if let Some(listener) = &self.listener {
            drop(listener);
            self.listener = None;
        }
        Ok(())
    }

    async fn dial(&self, addr: &net::SocketAddr) -> io::Result<()> {
        let conn = TcpStream::connect(addr).await?;
        let peer = TCPPeer::new(conn, true);
        let handshake_func = self.opts.handshake_func;
        handshake_func(peer.clone())?;
        if let Some(on_peer) = self.opts.on_peer {
            on_peer(peer)?;
        }
        Ok(())
    }

    async fn listen_and_accept(&mut self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.opts.listen_addr).await?;
        self.listener = Some(listener);
        let listener = self.listener.as_ref().unwrap();
        loop {
            let (stream, _) = listener.accept().await?;
            let peer = TCPPeer::new(stream, false);
            let handshake_func = self.opts.handshake_func;
            handshake_func(peer.clone())?;
            if let Some(on_peer) = self.opts.on_peer {
                on_peer(peer)?;
            }
        }
    }
}
