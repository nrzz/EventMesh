using EventMesh.Abstractions.Envelope;
using FluentAssertions;

namespace EventMesh.Core.Tests;

public sealed class MessageEnvelopeTests
{
    [Fact]
    public void Create_ReturnsBuilderWithGeneratedIdAndTimestamp()
    {
        var envelope = MessageEnvelope.Create()
            .WithSource("urn:eventmesh:test")
            .WithType("test.created")
            .Build();

        envelope.Id.Should().NotBeNullOrWhiteSpace();
        envelope.Time.Should().NotBeNull();
        envelope.SpecVersion.Should().Be("1.0");
    }

    [Fact]
    public void From_CopiesExistingEnvelope()
    {
        var original = MessageEnvelope.Create()
            .WithId("event-1")
            .WithSource("urn:eventmesh:source")
            .WithType("order.placed")
            .WithSubject("orders/1")
            .WithCorrelationId("corr-1")
            .WithCausationId("cause-1")
            .WithDataContentType("application/json")
            .WithData(new byte[] { 1, 2, 3 })
            .WithHeader("tenant", "acme")
            .Build();

        var copy = MessageEnvelope.From(original).Build();

        copy.Should().BeEquivalentTo(original, options => options.ComparingByMembers<MessageEnvelope>());
    }

    [Fact]
    public void WithHeaders_ReplacesHeadersOnCopy()
    {
        var original = MessageEnvelope.Create()
            .WithSource("urn:eventmesh:source")
            .WithType("test.event")
            .WithHeader("original", "value")
            .Build();

        var updated = original.WithHeaders(new Dictionary<string, string>
        {
            ["replacement"] = "new-value",
        });

        updated.Should().NotBeSameAs(original);
        updated.Headers.Should().ContainKey("replacement").WhoseValue.Should().Be("new-value");
        original.Headers.Should().ContainKey("original");
    }

    [Fact]
    public void Build_RequiresSourceTypeAndId()
    {
        var builder = MessageEnvelope.Create()
            .WithSource(" ")
            .WithType("test.event");

        var action = () => builder.Build();

        action.Should().Throw<InvalidOperationException>()
            .WithMessage("*Source*");
    }
}
