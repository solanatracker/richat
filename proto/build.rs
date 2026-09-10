fn main() -> anyhow::Result<()> {
    // Use vendored protoc to avoid building C++ protobuf via autotools
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

    // build protos
    generate_transport()
}

fn generate_transport() -> anyhow::Result<()> {
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        // Yellowstone types are provided by `yellowstone-grpc-proto` crate,
        // vendored `.proto` files are used only to resolve imports.
        .extern_path(".geyser", "::yellowstone_grpc_proto::geyser")
        .extern_path(
            ".solana.storage.ConfirmedBlock",
            "::yellowstone_grpc_proto::solana::storage::confirmed_block",
        )
        .compile_protos(&["proto/richat.proto"], &["proto", "proto/yellowstone"])?;

    Ok(())
}
