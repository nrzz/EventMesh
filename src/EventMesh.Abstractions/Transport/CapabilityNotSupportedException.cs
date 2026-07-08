namespace EventMesh.Abstractions.Transport;

/// <summary>
/// Thrown when an operation requires broker capabilities that are not supported by the active transport.
/// </summary>
public sealed class CapabilityNotSupportedException : InvalidOperationException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CapabilityNotSupportedException"/> class.
    /// </summary>
    /// <param name="capability">The unsupported capability.</param>
    /// <param name="transportName">The transport that does not support the capability.</param>
    public CapabilityNotSupportedException(BrokerCapabilities capability, string transportName)
        : base($"Transport '{transportName}' does not support the '{capability}' capability.")
    {
        Capability = capability;
        TransportName = transportName;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CapabilityNotSupportedException"/> class.
    /// </summary>
    /// <param name="capability">The unsupported capability.</param>
    /// <param name="transportName">The transport that does not support the capability.</param>
    /// <param name="message">The exception message.</param>
    public CapabilityNotSupportedException(BrokerCapabilities capability, string transportName, string message)
        : base(message)
    {
        Capability = capability;
        TransportName = transportName;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CapabilityNotSupportedException"/> class.
    /// </summary>
    /// <param name="capability">The unsupported capability.</param>
    /// <param name="transportName">The transport that does not support the capability.</param>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public CapabilityNotSupportedException(
        BrokerCapabilities capability,
        string transportName,
        string message,
        Exception innerException)
        : base(message, innerException)
    {
        Capability = capability;
        TransportName = transportName;
    }

    /// <summary>
    /// Gets the capability that is not supported.
    /// </summary>
    public BrokerCapabilities Capability { get; }

    /// <summary>
    /// Gets the name of the transport that does not support the capability.
    /// </summary>
    public string TransportName { get; }
}
