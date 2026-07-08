namespace EventMesh.Abstractions.Transport;

/// <summary>
/// Extension methods for working with <see cref="BrokerCapabilities"/>.
/// </summary>
public static class BrokerCapabilitiesExtensions
{
    /// <summary>
    /// Determines whether all specified capabilities are supported.
    /// </summary>
    public static bool SupportsAll(this BrokerCapabilities capabilities, BrokerCapabilities required) =>
        (capabilities & required) == required;

    /// <summary>
    /// Determines whether any of the specified capabilities are supported.
    /// </summary>
    public static bool SupportsAny(this BrokerCapabilities capabilities, BrokerCapabilities required) =>
        (capabilities & required) != 0;

    /// <summary>
    /// Returns the capabilities that are missing from the current set.
    /// </summary>
    public static BrokerCapabilities GetMissing(this BrokerCapabilities capabilities, BrokerCapabilities required) =>
        required & ~capabilities;
}
