# cuddlyFS

A distributed file system implemented in Rust.

## Table of Contents

-   [Introduction](#introduction)
-   [Features](#features)
-   [Architecture](#architecture)
-   [Installation](#installation)
-   [Usage](#usage)
-   [Tutorial](#tutorial)
-   [Configuration](#configuration)
-   [Development](#development)
-   [Contributing](#contributing)
-   [License](#license)
-   [Authors](#authors)

## Introduction

cuddlyFS is a distributed file system designed to provide high availability, scalability, and performance. It is implemented in Rust and offers a cuddly API for easy integration with your applications.

## Features

-   Distributed architecture
-   High availability and fault tolerance
-   Scalability to handle large amounts of data
-   Efficient data storage and retrieval
-   Secure communication between nodes
-   Easy configuration and deployment

## Architecture

cuddlyFS consists of several components:

-   **NameNode**: Manages the metadata of the file system, including the directory structure and file locations.
-   **DataNode**: Stores the actual data blocks and handles read/write requests from clients.
-   **Client**: Interacts with the NameNode and DataNodes to perform file operations.

The communication between these components is handled using gRPC, and the protocol definitions can be found in the `proto` directory.

## Installation

Make sure that protoc is installed on your system. If not, you can install it by following the instructions [here](https://grpc.io/docs/protoc-installation/).

Or run the following command:

```bash
# For Ubuntu
sudo apt install protobuf-compiler

# For Fedora
sudo dnf install protobuf
```

To install cuddlyFS with cargo:

```sh
cargo install cuddlyFS
```

To install cuddlyFS as a library, add it to your `Cargo.toml`:

```toml
[dependencies]
cuddlyfs = "0.1.2"
```

or run:

```sh
cargo add cuddlyfs
```

To compile cuddlyFS from source, follow these steps:

1. Clone the repository:

    ```sh
    git clone https://github.com/niraj-kumar-r/cuddlyFS.git
    cd cuddlyFS
    ```

2. Build the project using Cargo:

    ```sh
    cargo build --release
    ```

3. Set up the configuration files in the `config` directory.

## Usage

For detailed usage instructions, refer to the [rust docs](https://docs.rs/cuddlyfs/0.1.2/cuddlyfs/)

To start the NameNode and DataNodes, use the following commands:

1. Start the NameNode:

    ```sh
    cargo run --bin namenode
    ```

2. Start the DataNodes:

    ```sh
    cargo run --bin datanode
    ```

3. Use the client to interact with the file system:
    ```sh
    cargo run --bin client
    ```

It can also be run inside a docker container. The dockerfile is provided in the repository.

A simple compose file is also provided to start a basic cluster of NameNode and DataNodes.

```sh
# Start the cluster
docker-compose up
```

## Tutorial

To get started with cuddlyFS, follow these steps:

1. Clone the repository and build the project (see the [Installation](#installation) section for instructions).

2. Make sure Protobuf and Docker Desktop are installed on your system, and Docker Daemon is running.

3. Spin up a cluster of NameNode and DataNodes using Docker Compose:

    ```sh
    docker-compose up
    ```

4. Use the client to interact with the file system:

    ```sh
    cargo run --bin cuddly_client report
    ```

5. Create a new file:

    ```sh
    cargo run --bin cuddly_client -- put <local_file_path> /<remote_file_path>
    ```

6. List the files in the file system:

    ```sh
    cargo run --bin cuddly_client -- ls /
    ```

7. Get the contents of a file:

    ```sh
    cargo run --bin cuddly_client -- get /<remote_file_path> <local_file_path>
    ```

## Configuration

The configuration files for the NameNode and DataNodes can be found in the `config` directory. You can modify these files to customize the behavior of the components.

Do not modify the default configuration files in the `config` directory. Instead, create a new configuration file and pass it as a command-line argument when starting the components:

```sh
# for dev.yaml file in config directory
EXPORT RUN_MODE=dev
cargo run --bin namenode
```

## Development

To contribute to cuddlyFS, fork the repository and create a new branch for your changes. Make sure to follow the [Rust style guide](https://doc.rust-lang.org/1.0.0/style/).

After making your changes, run the tests to ensure that everything is working correctly:

```sh
cargo test
```

If the tests pass, submit a pull request with your changes.

## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request if you have any suggestions, bug reports, or feature requests.

## License

This project is licensed under the Apache License-2.0 - see the [LICENSE](LICENSE) file for details.

## Authors

-   [Niraj Kumar](https://github.com/niraj-kumar-r)
