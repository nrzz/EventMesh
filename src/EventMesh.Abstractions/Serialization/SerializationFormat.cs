namespace EventMesh.Abstractions.Serialization;

/// <summary>
/// Supported message serialization formats.
/// </summary>
public enum SerializationFormat
{
    /// <summary>
    /// JSON serialization.
    /// </summary>
    Json = 0,

    /// <summary>
    /// MessagePack binary serialization.
    /// </summary>
    MessagePack = 1,

    /// <summary>
    /// Apache Avro serialization.
    /// </summary>
    Avro = 2,

    /// <summary>
    /// Protocol Buffers serialization.
    /// </summary>
    Protobuf = 3,
}
