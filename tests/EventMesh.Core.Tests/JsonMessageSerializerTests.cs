using EventMesh.Core.Serialization;
using FluentAssertions;

namespace EventMesh.Core.Tests;

public sealed class JsonMessageSerializerTests
{
    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public void Format_IsJson()
    {
        _serializer.Format.Should().Be(Abstractions.Serialization.SerializationFormat.Json);
    }

    [Fact]
    public void DefaultContentType_IsApplicationJson()
    {
        _serializer.DefaultContentType.Should().Be("application/json");
    }

    [Fact]
    public async Task SerializeAndDeserialize_RoundTripsMessage()
    {
        var message = new SampleMessage
        {
            Id = 42,
            Name = "eventmesh",
        };

        var bytes = await _serializer.SerializeAsync(message);
        var restored = await _serializer.DeserializeAsync<SampleMessage>(bytes);

        restored.Should().BeEquivalentTo(message);
    }

    [Fact]
    public async Task Deserialize_NullPayload_Throws()
    {
        var nullJson = new ReadOnlyMemory<byte>(System.Text.Encoding.UTF8.GetBytes("null"));

        var action = async () => await _serializer.DeserializeAsync<SampleMessage>(nullJson);

        await action.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*deserialize*");
    }

    [Fact]
    public async Task Serialize_RespectsCancellation()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var action = async () => await _serializer.SerializeAsync(new SampleMessage { Id = 1, Name = "x" }, cancellationToken: cts.Token);

        await action.Should().ThrowAsync<OperationCanceledException>();
    }

    private sealed class SampleMessage
    {
        public int Id { get; init; }

        public string Name { get; init; } = string.Empty;
    }
}
