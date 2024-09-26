use std::net::SocketAddr;

use cuddlyfs::namenode::Namenode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "[::1]:50051".parse().unwrap();
    let namenode: Namenode = Namenode::default();

    namenode.run(addr).await?;

    Ok(())
}
