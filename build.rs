fn main() {
    tonic_build::configure()
        .compile(
            &[
                "proto/common.proto",
                "proto/auth.proto",
                "proto/file.proto",
                "proto/node.proto",
                "proto/directory.proto",
            ],
            &["proto"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
