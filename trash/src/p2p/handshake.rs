use crate::p2p::Peer;
use std::io;

pub type HandshakeFunc = fn(&dyn Peer) -> Result<(), io::Error>;

pub const NOP_HANDSHAKE_FUNC: HandshakeFunc = |_peer: &dyn Peer| Ok(());
