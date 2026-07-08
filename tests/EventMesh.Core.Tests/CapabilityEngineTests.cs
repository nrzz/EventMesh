using EventMesh.Abstractions.Transport;
using EventMesh.Core.Capabilities;
using FluentAssertions;

namespace EventMesh.Core.Tests;

public sealed class CapabilityEngineTests
{
    [Fact]
    public void Require_AddsRequiredCapabilities()
    {
        var engine = new CapabilityEngine();

        engine.Require(BrokerCapabilities.Replay);
        engine.Require(BrokerCapabilities.DeadLettering);

        engine.RequiredCapabilities.Should().Be(BrokerCapabilities.Replay | BrokerCapabilities.DeadLettering);
    }

    [Fact]
    public void EnableEmulation_AddsEmulatedCapabilities()
    {
        var engine = new CapabilityEngine();

        engine.EnableEmulation(BrokerCapabilities.DelayedDelivery);

        engine.EmulatedCapabilities.Should().Be(BrokerCapabilities.DelayedDelivery);
    }

    [Fact]
    public void Validate_WhenTransportSupportsRequired_DoesNotThrow()
    {
        var engine = new CapabilityEngine();
        engine.Require(BrokerCapabilities.PubSub);

        var transport = new StubTransport(BrokerCapabilities.PubSub | BrokerCapabilities.Queues);

        var action = () => engine.Validate(transport);

        action.Should().NotThrow();
    }

    [Fact]
    public void Validate_WhenCapabilityIsEmulated_DoesNotThrow()
    {
        var engine = new CapabilityEngine();
        engine.Require(BrokerCapabilities.Replay);
        engine.EnableEmulation(BrokerCapabilities.Replay);

        var transport = new StubTransport(BrokerCapabilities.PubSub);

        var action = () => engine.Validate(transport);

        action.Should().NotThrow();
    }

    [Fact]
    public void Validate_WhenCapabilityMissing_ThrowsCapabilityNotSupportedException()
    {
        var engine = new CapabilityEngine();
        engine.Require(BrokerCapabilities.Transactions);

        var transport = new StubTransport(BrokerCapabilities.PubSub);

        var action = () => engine.Validate(transport);

        action.Should().Throw<CapabilityNotSupportedException>()
            .Which.Capability.Should().Be(BrokerCapabilities.Transactions);
    }

    [Fact]
    public void IsNativelySupported_ReturnsTrueWhenTransportSupportsCapability()
    {
        var engine = new CapabilityEngine();
        var transport = new StubTransport(BrokerCapabilities.Ordering);

        engine.IsNativelySupported(transport, BrokerCapabilities.Ordering).Should().BeTrue();
        engine.IsNativelySupported(transport, BrokerCapabilities.Replay).Should().BeFalse();
    }

    [Fact]
    public void WillEmulate_ReturnsTrueWhenEmulationEnabledAndNotNative()
    {
        var engine = new CapabilityEngine();
        engine.EnableEmulation(BrokerCapabilities.Replay);

        var transport = new StubTransport(BrokerCapabilities.PubSub);

        engine.WillEmulate(transport, BrokerCapabilities.Replay).Should().BeTrue();
        engine.WillEmulate(transport, BrokerCapabilities.PubSub).Should().BeFalse();
    }

    [Fact]
    public void Validate_NullTransport_Throws()
    {
        var engine = new CapabilityEngine();

        var action = () => engine.Validate(null!);

        action.Should().Throw<ArgumentNullException>();
    }

    private sealed class StubTransport(BrokerCapabilities capabilities) : IBrokerTransport
    {
        public string Name => "stub";

        public BrokerCapabilities GetCapabilities() => capabilities;

        public Task<TransportSendResult> SendAsync(TransportMessage message, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransportSendResult.Success("stub-message"));

        public Task<TransportReceiveResult> ReceiveAsync(string queueOrSubscription, CancellationToken cancellationToken = default) =>
            Task.FromResult(TransportReceiveResult.Empty());

        public Task AcknowledgeAsync(string deliveryTag, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task RejectAsync(string deliveryTag, bool requeue = false, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task CreateTopologyAsync(TopologyDefinition topology, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
