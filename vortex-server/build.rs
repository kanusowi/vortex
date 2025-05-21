fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Define the paths to your .proto files.
    // These paths are relative to the `vortex-proto` directory,
    // and we'll specify an include path for `vortex-proto/src/proto`.
    let proto_files = &[
        "../vortex-proto/src/proto/vortex/api/v1/common.proto",
        "../vortex-proto/src/proto/vortex/api/v1/collections_service.proto",
        "../vortex-proto/src/proto/vortex/api/v1/points_service.proto",
        "../vortex-proto/src/proto/vortex/api/v1/snapshots_service.proto",
    ];

    // Define the include path for resolving imports.
    // This should point to the directory where `google/protobuf/struct.proto`
    // can be found (via the `googleapis` dependency fetched by buf) and
    // where our own proto files are, relative to their import statements.
    // `tonic-build` needs to find `google/protobuf/struct.proto`.
    // The `buf mod update` command should have downloaded `googleapis` into
    // `vortex-proto/buf/build/googleapis/googleapis`.
    // Our own protos are in `vortex-proto/src/proto`.
    // The import `src/proto/vortex/api/v1/common.proto` needs `vortex-proto` as an include path.
    // The import `google/protobuf/struct.proto` needs `vortex-proto/buf/build/googleapis/googleapis` as an include path.
    // For imports like `import "vortex/api/v1/common.proto";` within proto files,
    // the include path should point to the directory containing the top-level package directory (e.g., `vortex`).
    // So, `../vortex-proto/src/proto` should be an include path.

    let include_paths = &[
        "../vortex-proto/src/proto", // For resolving "vortex/api/v1/common.proto" from other protos
        "../vortex-proto/buf/build/googleapis/googleapis/", // For "google/protobuf/struct.proto"
    ];

    tonic_build::configure()
        .build_server(true) // Generate server code
        .build_client(true) // We might enable client later for integration tests within this crate
        .compile_well_known_types(true) // Needed for google.protobuf.Value -> prost_types::Value
        .extern_path(".google.protobuf.Value", "::prost_types::Value")
        .extern_path(".google.protobuf.Struct", "::prost_types::Struct")
        .extern_path(".google.protobuf.ListValue", "::prost_types::ListValue")
        .extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp")
        .compile(proto_files, include_paths)?;

    Ok(())
}
