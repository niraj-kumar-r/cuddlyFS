use tokio::task;
use tokio::time::{interval, Duration};

use cuddlyfs::datanode::{self, Datanode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let datanode = Datanode::new();

    let datanode_clone = datanode.clone();

    let handle = task::spawn(async move {
        let mut interval = interval(Duration::from_secs(3));
        loop {
            interval.tick().await;
            match datanode_clone.heartbeat().await {
                Ok(_) => println!("Heartbeat sent successfully"),
                Err(e) => eprintln!("Failed to send heartbeat: {:?}", e),
            }
        }
    });

    print!("Main thread is free");

    handle.await.unwrap();

    Ok(())
}
