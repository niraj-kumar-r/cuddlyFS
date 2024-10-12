use std::env;

use cuddlyfs::datanode::Datanode;
use log::info;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    let (shutdown_send, mut shutdown_recv) = mpsc::unbounded_channel::<i8>();
    let cancel_token: CancellationToken = CancellationToken::new();
    let datanode: Datanode = Datanode::new(cancel_token.clone(), shutdown_send);

    let running_datanode_handle = tokio::spawn(async move {
        let _ = datanode.run().await;
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Ctrl-C received, shutting down...");
        },
        _ = shutdown_recv.recv() => {
            info!("Received shutdown signal");
        },
    }

    cancel_token.cancel();

    let _ = running_datanode_handle.await;

    info!("Datanode shut down successfully");

    Ok(())
}
