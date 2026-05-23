fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        if let Ok(p) = protoc_bin_vendored::protoc_bin_path() {
            std::env::set_var("PROTOC", p);
        }
    }
    let proto_dir = "../../proto";
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                format!("{}/raft.proto", proto_dir),
                format!("{}/membership.proto", proto_dir),
                format!("{}/admin.proto", proto_dir),
            ],
            &[proto_dir.to_string()],
        )?;
    Ok(())
}
