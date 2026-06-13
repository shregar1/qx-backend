fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate `bytes::Bytes` instead of `Vec<u8>` for the `bytes`
    // fields that carry record payloads and keys on the Produce
    // path. Refcounted slices let the forwarding path (a non-leader
    // broker proxying Produce to the leader's gRPC) hand the original
    // buffer to tonic without an intermediate `Vec<u8>` copy. Prost's
    // encoder accepts `bytes::Bytes` directly.
    //
    // Selective opt-in: the rest of the proto surface stays on
    // `Vec<u8>` to avoid churning ergonomics for fields that aren't
    // on a hot allocation path.
    tonic_build::configure()
        .bytes([
            // Produce path — already justified by the forwarding hot
            // path (avoids `Vec<u8>` cloning for keys + payloads).
            ".qx.v1.SendRequest.key",
            ".qx.v1.SendRequest.payload",
            ".qx.v1.SendItem.key",
            ".qx.v1.SendItem.payload",
            // Receive path — `TopicMessage` and `StreamedRecord`
            // carry the broker's already-allocated record payload to
            // the client. Moving through prost as `Bytes` makes the
            // wire encoding step zero-copy on the broker side. The
            // qx-storage layer now surfaces `Bytes` directly from
            // `Segment::read`, so no per-record conversion happens
            // anywhere in the read path.
            ".qx.v1.TopicMessage.payload",
            ".qx.v1.StreamedRecord.payload",
            // Workflow task payload + result — qx-workflow surfaces
            // them as `Bytes` end-to-end, so prost can encode them
            // refcount-shared instead of memcpying through Vec<u8>.
            ".qx.v1.TaskMessage.payload",
            ".qx.v1.TaskMessage.result",
            ".qx.v1.TaskMessage.error",
            // Workflow task creation + completion bytes — keep the
            // refcounted-buffer path symmetric on both sides of the
            // RPC. CompleteTaskWithEmits ferries one result plus N
            // emit payloads; both flow through as Bytes so the
            // broker's outbox writer never re-buffers them.
            ".qx.v1.CreateRootTaskRequest.payload",
            ".qx.v1.CreateChildTaskRequest.payload",
            ".qx.v1.CompleteTaskRequest.result",
            ".qx.v1.CompleteTaskWithEmitsRequest.result",
            ".qx.v1.TopicEmit.payload",
        ])
        .compile_protos(&["proto/qx.proto"], &["proto"])?;
    Ok(())
}
