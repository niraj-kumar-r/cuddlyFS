pub trait Peer {
    fn send(&self, msg: &str);
    fn recv(&self) -> String;
}
