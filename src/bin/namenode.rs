use std::net::SocketAddr;

use cuddlyfs::namenode::Namenode;
use tokio::signal;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "[::1]:50051".parse().unwrap();
    let cancel_token = CancellationToken::new();
    let namenode: Namenode = Namenode::new(cancel_token.clone());

    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("\nCtrl-C received, shutting down...\n");
            cancel_token.cancel();
        },
        _ = namenode.run(addr) => {},
    }

    Ok(())
}
