using EventMesh.Abstractions.Transport;

namespace EventMesh.Core.Capabilities;

/// <summary>
/// Validates broker capabilities against required features at configuration time.
/// </summary>
public sealed class CapabilityEngine
{
    private BrokerCapabilities _requiredCapabilities;
    private BrokerCapabilities _emulatedCapabilities;

    /// <summary>
    /// Gets the capabilities required by the current application configuration.
    /// </summary>
    public BrokerCapabilities RequiredCapabilities => _requiredCapabilities;

    /// <summary>
    /// Gets the capabilities that will be emulated by the runtime.
    /// </summary>
    public BrokerCapabilities EmulatedCapabilities => _emulatedCapabilities;

    /// <summary>
    /// Marks a capability as required by the application configuration.
    /// </summary>
    public void Require(BrokerCapabilities capability)
    {
        _requiredCapabilities |= capability;
    }

    /// <summary>
    /// Marks a capability as emulated when not natively supported by the transport.
    /// </summary>
    public void EnableEmulation(BrokerCapabilities capability)
    {
        _emulatedCapabilities |= capability;
    }

    /// <summary>
    /// Validates that the transport supports or can emulate all required capabilities.
    /// </summary>
    /// <param name="transport">The broker transport to validate.</param>
    /// <exception cref="CapabilityNotSupportedException">
    /// Thrown when required capabilities are neither supported nor emulated.
    /// </exception>
    public void Validate(IBrokerTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);

        var supported = transport.GetCapabilities();
        var satisfiable = supported | _emulatedCapabilities;
        var missing = satisfiable.GetMissing(_requiredCapabilities);

        if (missing == BrokerCapabilities.None)
        {
            return;
        }

        var firstMissing = GetFirstFlag(missing);
        throw new CapabilityNotSupportedException(
            firstMissing,
            transport.Name,
            $"Transport '{transport.Name}' does not support required capability '{firstMissing}'. " +
            $"Supported: {supported}, Emulated: {_emulatedCapabilities}, Missing: {missing}.");
    }

    /// <summary>
    /// Determines whether the transport natively supports the specified capability.
    /// </summary>
    public bool IsNativelySupported(IBrokerTransport transport, BrokerCapabilities capability) =>
        transport.GetCapabilities().SupportsAll(capability);

    /// <summary>
    /// Determines whether the specified capability will be emulated for the transport.
    /// </summary>
    public bool WillEmulate(IBrokerTransport transport, BrokerCapabilities capability) =>
        !transport.GetCapabilities().SupportsAll(capability) && _emulatedCapabilities.SupportsAll(capability);

    private static BrokerCapabilities GetFirstFlag(BrokerCapabilities capabilities)
    {
        foreach (BrokerCapabilities value in Enum.GetValues<BrokerCapabilities>())
        {
            if (value != BrokerCapabilities.None && capabilities.SupportsAll(value))
            {
                return value;
            }
        }

        return capabilities;
    }
}
