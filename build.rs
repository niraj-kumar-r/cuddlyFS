fn main() {
    tonic_build::configure()
        .compile_protos(
            &[
                "proto/common.proto",
                "proto/auth.proto",
                "proto/file.proto",
                "proto/node.proto",
                "proto/directory.proto",
                "proto/datanode.proto",
                "proto/namenode.proto",
            ],
            &["proto"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
