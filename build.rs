fn main() {
    tonic_build::configure()
        .bytes(&["."])
        .compile_protos(
            &[
                "proto/common.proto",
                "proto/auth.proto",
                "proto/file.proto",
                "proto/node.proto",
                "proto/directory.proto",
                "proto/datanode.proto",
                "proto/namenode.proto",
                "proto/client_to_datanode.proto",
                "proto/datanode_to_datanode.proto",
            ],
            &["proto"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
