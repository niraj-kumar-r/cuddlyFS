use std::process::exit;

use clap::{arg, command, Command};
use cuddlyfs::{
    errors::{CuddlyError, CuddlyResult},
    fs_client::CuddlyClient,
    APP_CONFIG,
};

#[tokio::main]
async fn main() -> CuddlyResult<()> {
    let namenode_rpc_address = APP_CONFIG.datanode.namenode_rpc_address.clone();

    let dfs = CuddlyClient::new(namenode_rpc_address).await?;

    let matches = command!()
        .subcommand(Command::new("report").about("Reports basic filesystem information."))
        .subcommand(
            Command::new("ls")
                .about("Lists the content of a given directory.")
                .arg(arg!(<path> "The complete path to the directory.")),
        )
        .subcommand(
            Command::new("mkdir")
                .about("Creates a new directory (equivalent to `mkdir -p` on Unix systems).")
                .arg(arg!(<path> "The directory to create.")),
        )
        .subcommand(
            Command::new("put")
                .about("Uploads a local file from `src` to remote `dst`")
                .arg(arg!(<src> "Path to local file"))
                .arg(arg!(<dst> "Path to remote file")),
        )
        .subcommand(
            Command::new("get")
                .about("Downloads a remote file `src` to local destination `dst`")
                .arg(arg!(<src> "Path to remote file"))
                .arg(arg!(<dst> "Path to local file")),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("report", _)) => {
            let datanodes = dfs.nodes_report().await?;
            println!("Datanodes:");
            for datanode in datanodes {
                let used_percentage = 100.0 * (datanode.used_capacity as f64)
                    / (datanode.total_capacity - datanode.used_capacity) as f64
                    + datanode.used_capacity as f64;
                println!("\tAddress: {}", datanode.ip_address);
                println!(
                    "\tAvailable storage (kB): {}",
                    (datanode.total_capacity - datanode.used_capacity)
                );
                println!(
                    "\tUsed storage (kB): {} ({:.2}%)",
                    datanode.used_capacity, used_percentage
                );
                println!();
            }
        }
        Some(("ls", sub_matches)) => {
            let path = sub_matches
                .get_one::<String>("path")
                .ok_or_else(|| CuddlyError::ArgMissingError("Path required".to_owned()))?;
            let mut files = dfs.ls(path).await?;
            files.sort();
            for file in files {
                println!("{}", file);
            }
        }
        Some(("mkdir", sub_matches)) => {
            let path = sub_matches
                .get_one::<String>("path")
                .ok_or_else(|| CuddlyError::ArgMissingError("Path required".to_owned()))?;
            dfs.mkdir(path).await?;
        }
        Some(("put", sub_matches)) => {
            let src = sub_matches
                .get_one::<String>("src")
                .ok_or_else(|| CuddlyError::ArgMissingError("Source required".to_owned()))?;
            let dst = sub_matches
                .get_one::<String>("dst")
                .ok_or_else(|| CuddlyError::ArgMissingError("Destination required".to_owned()))?;
            dfs.put(src, dst).await?;
        }
        Some(("get", sub_matches)) => {
            let src = sub_matches
                .get_one::<String>("src")
                .ok_or_else(|| CuddlyError::ArgMissingError("Source required".to_owned()))?;
            let dst = sub_matches
                .get_one::<String>("dst")
                .ok_or_else(|| CuddlyError::ArgMissingError("Destination required".to_owned()))?;
            dfs.get(src, dst).await?;
        }
        Some((subcommand, _)) => {
            eprintln!("Unrecognized command: '{}'", subcommand);
            exit(1);
        }
        None => {
            eprintln!("No subcommand was used.");
            exit(1);
        }
    }

    Ok(())
}
