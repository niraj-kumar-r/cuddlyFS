use std::{env, net::SocketAddr};

use cuddlyfs::namenode::Namenode;
use log::info;
use tokio::{signal, sync::mpsc};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    let addr: SocketAddr = "[::1]:50051".parse().unwrap();
    let (shutdown_send, mut shutdown_recv) = mpsc::unbounded_channel::<i8>();
    let cancel_token = CancellationToken::new();
    let namenode: Namenode = Namenode::new(cancel_token.clone(), shutdown_send);

    let running_namenode_handle = tokio::spawn(async move {
        let _ = namenode.run(addr).await;
    });

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("Ctrl-C received, shutting down...");
        },
        _ = shutdown_recv.recv() => {
            info!("Received shutdown signal");
        },
    }

    cancel_token.cancel();

    let _ = running_namenode_handle.await;

    info!("Namenode shut down successfully");

    Ok(())
}
