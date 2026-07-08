using EventMesh.Abstractions.Envelope;
using FluentAssertions;

namespace EventMesh.Abstractions.Tests;

public sealed class MessageEnvelopeBuilderTests
{
    [Fact]
    public void DefaultConstructor_GeneratesIdAndTimestamp()
    {
        var envelope = new MessageEnvelopeBuilder()
            .WithSource("urn:eventmesh:test")
            .WithType("test.created")
            .Build();

        envelope.Id.Should().NotBeNullOrWhiteSpace();
        envelope.Time.Should().NotBeNull();
        envelope.SpecVersion.Should().Be("1.0");
    }

    [Fact]
    public void Build_RequiresSourceTypeAndId()
    {
        var action = () => new MessageEnvelopeBuilder()
            .WithType("test.created")
            .Build();

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*Source*");
    }

    [Fact]
    public void WithHeader_IsCaseInsensitive()
    {
        var envelope = new MessageEnvelopeBuilder()
            .WithSource("urn:eventmesh:test")
            .WithType("test.created")
            .WithHeader("Tenant", "acme")
            .WithHeader("tenant", "override")
            .Build();

        envelope.Headers.Should().ContainKey("tenant").WhoseValue.Should().Be("override");
        envelope.Headers.Count.Should().Be(1);
    }

    [Fact]
    public void WithHeaders_MergesMultipleHeaders()
    {
        var envelope = new MessageEnvelopeBuilder()
            .WithSource("urn:eventmesh:test")
            .WithType("test.created")
            .WithHeaders(
            [
                new KeyValuePair<string, string>("a", "1"),
                new KeyValuePair<string, string>("b", "2"),
            ])
            .Build();

        envelope.Headers.Should().ContainKeys("a", "b");
    }

    [Fact]
    public void CopyConstructor_SeedsFromExistingEnvelope()
    {
        var original = new MessageEnvelopeBuilder()
            .WithId("event-42")
            .WithSource("urn:eventmesh:orders")
            .WithType("order.created")
            .WithSubject("orders/42")
            .WithCorrelationId("corr")
            .WithCausationId("cause")
            .WithDataContentType("application/json")
            .WithData(new byte[] { 9 })
            .WithHeader("region", "us-east")
            .Build();

        var copy = new MessageEnvelopeBuilder(original).Build();

        copy.Should().BeEquivalentTo(original, options => options.ComparingByMembers<MessageEnvelope>());
    }

    [Fact]
    public void WithData_AcceptsByteArray()
    {
        var data = new byte[] { 1, 2, 3 };

        var envelope = new MessageEnvelopeBuilder()
            .WithSource("urn:eventmesh:test")
            .WithType("test.created")
            .WithData(data)
            .Build();

        envelope.Data.Should().NotBeNull();
        envelope.Data!.Value.ToArray().Should().Equal(data);
    }

    [Fact]
    public void CopyConstructor_NullEnvelope_Throws()
    {
        var action = () => new MessageEnvelopeBuilder(null!);

        action.Should().Throw<ArgumentNullException>();
    }
}
