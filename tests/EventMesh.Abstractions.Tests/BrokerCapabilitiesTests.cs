using EventMesh.Abstractions.Transport;
using FluentAssertions;

namespace EventMesh.Abstractions.Tests;

public sealed class BrokerCapabilitiesTests
{
    [Fact]
    public void SupportsAll_ReturnsTrueWhenAllFlagsPresent()
    {
        var capabilities = BrokerCapabilities.PubSub | BrokerCapabilities.Queues | BrokerCapabilities.Ordering;

        capabilities.SupportsAll(BrokerCapabilities.PubSub | BrokerCapabilities.Queues).Should().BeTrue();
        capabilities.SupportsAll(BrokerCapabilities.Replay).Should().BeFalse();
    }

    [Fact]
    public void SupportsAny_ReturnsTrueWhenAnyFlagPresent()
    {
        var capabilities = BrokerCapabilities.PubSub;

        capabilities.SupportsAny(BrokerCapabilities.PubSub | BrokerCapabilities.Replay).Should().BeTrue();
        capabilities.SupportsAny(BrokerCapabilities.Transactions).Should().BeFalse();
    }

    [Fact]
    public void GetMissing_ReturnsCapabilitiesNotPresent()
    {
        var capabilities = BrokerCapabilities.PubSub | BrokerCapabilities.Queues;
        var required = BrokerCapabilities.PubSub | BrokerCapabilities.Replay | BrokerCapabilities.Queues;

        capabilities.GetMissing(required).Should().Be(BrokerCapabilities.Replay);
    }

    [Fact]
    public void None_HasZeroValue()
    {
        BrokerCapabilities.None.Should().Be((BrokerCapabilities)0);
    }
}
