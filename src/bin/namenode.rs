use std::{env, net::SocketAddr};

use cuddlyfs::{errors::CuddlyResult, namenode::Namenode, APP_CONFIG};
use log::info;
use tokio::{signal, sync::mpsc};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> CuddlyResult<()> {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    let addr: SocketAddr = APP_CONFIG.namenode.bind_address.parse().unwrap();
    let (shutdown_send, mut shutdown_recv) = mpsc::unbounded_channel::<i8>();
    let cancel_token: CancellationToken = CancellationToken::new();
    let namenode: Namenode = Namenode::new(cancel_token.clone(), shutdown_send)?;

    let running_namenode_handle = tokio::spawn(async move {
        info!("Starting namenode on {}", addr);
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
