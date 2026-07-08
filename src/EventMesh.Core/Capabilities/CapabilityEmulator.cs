using EventMesh.Abstractions.Transport;

namespace EventMesh.Core.Capabilities;

/// <summary>
/// Emulates broker capabilities that are not natively supported by the active transport.
/// </summary>
public sealed class CapabilityEmulator
{
    private readonly CapabilityEngine _capabilityEngine;

    public CapabilityEmulator(CapabilityEngine capabilityEngine)
    {
        _capabilityEngine = capabilityEngine;
    }

    /// <summary>
    /// Determines whether message delay should be handled by the in-memory scheduler.
    /// </summary>
    public bool ShouldUseSchedulerForDelay(IBrokerTransport transport) =>
        _capabilityEngine.WillEmulate(transport, BrokerCapabilities.DelayedDelivery)
        || _capabilityEngine.WillEmulate(transport, BrokerCapabilities.NativeScheduling)
        || (!transport.GetCapabilities().SupportsAny(BrokerCapabilities.DelayedDelivery | BrokerCapabilities.NativeScheduling)
            && _capabilityEngine.EmulatedCapabilities.SupportsAny(BrokerCapabilities.DelayedDelivery | BrokerCapabilities.NativeScheduling));

    /// <summary>
    /// Determines whether dead-letter routing should use convention-based destinations.
    /// </summary>
    public bool ShouldUseConventionDeadLetter(IBrokerTransport transport) =>
        !transport.GetCapabilities().SupportsAll(BrokerCapabilities.DeadLettering)
        && _capabilityEngine.EmulatedCapabilities.SupportsAll(BrokerCapabilities.DeadLettering);

    /// <summary>
    /// Resolves the dead-letter destination for a source queue or topic.
    /// </summary>
    public string ResolveDeadLetterDestination(string sourceDestination) =>
        $"{sourceDestination}.error";

    /// <summary>
    /// Resolves the reply-to destination used for request/response messaging.
    /// </summary>
    public string ResolveReplyDestination(string applicationName) =>
        $"{applicationName}.replies";
}
