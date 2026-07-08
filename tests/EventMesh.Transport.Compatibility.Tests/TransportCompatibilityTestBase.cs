using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using FluentAssertions;

namespace EventMesh.Transport.Compatibility.Tests;

/// <summary>
/// Shared compatibility tests that every transport adapter must pass.
/// </summary>
public abstract class TransportCompatibilityTestBase
{
    protected const string TestTopic = "compat.events";
    protected const string TestQueue = "compat.queue";
    protected const string TestSubscription = "compat.subscription";
    protected const string TestDlq = "compat.queue.dlq";
    protected const string ReplyQueue = "compat.replies";

    protected abstract Task<IBrokerTransport> CreateTransportAsync();

    protected abstract Task<TransportCompatibilityContext> CreateContextAsync();

    protected virtual TimeSpan DefaultReceiveTimeout => TimeSpan.FromSeconds(5);

    [Fact]
    public async Task PublishSubscribe_RoundTrip_DeliversMessage()
    {
        var context = await CreateContextAsync();
        await using var transport = await CreateTransportAsync();

        await transport.CreateTopologyAsync(context.Topology, CancellationToken.None);

        var body = "hello-compat"u8.ToArray();
        var sendResult = await transport.SendAsync(new TransportMessage
        {
            Destination = TestTopic,
            RoutingKey = "created",
            Body = body,
            ContentType = "text/plain",
        }, CancellationToken.None);

        sendResult.Succeeded.Should().BeTrue();

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        cts.CancelAfter(DefaultReceiveTimeout);

        var receiveResult = await transport.ReceiveAsync(TestSubscription, cts.Token);
        receiveResult.HasMessage.Should().BeTrue();
        receiveResult.Message!.Body.ToArray().Should().Equal(body);
        receiveResult.Message.RoutingKey.Should().Be("created");

        await transport.AcknowledgeAsync(receiveResult.DeliveryTag!, cts.Token);
    }

    [Fact]
    public async Task RequestResponse_Correlation_ReturnsReply()
    {
        var context = await CreateContextAsync();
        await using var transport = await CreateTransportAsync();

        await transport.CreateTopologyAsync(context.Topology, CancellationToken.None);

        var correlationId = Guid.NewGuid().ToString("N");
        var requestBody = "ping"u8.ToArray();
        var responseBody = "pong"u8.ToArray();

        var requestTask = Task.Run(async () =>
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
            cts.CancelAfter(DefaultReceiveTimeout);

            var request = await transport.ReceiveAsync(TestSubscription, cts.Token);
            request.HasMessage.Should().BeTrue();
            request.Message!.CorrelationId.Should().Be(correlationId);
            request.Message.ReplyTo.Should().Be(ReplyQueue);

            await transport.SendAsync(new TransportMessage
            {
                Destination = ReplyQueue,
                Body = responseBody,
                CorrelationId = correlationId,
                ContentType = "text/plain",
            }, cts.Token);

            await transport.AcknowledgeAsync(request.DeliveryTag!, cts.Token);
        }, CancellationToken.None);

        var sendResult = await transport.SendAsync(new TransportMessage
        {
            Destination = TestTopic,
            Body = requestBody,
            CorrelationId = correlationId,
            ReplyTo = ReplyQueue,
            ContentType = "text/plain",
        }, CancellationToken.None);

        sendResult.Succeeded.Should().BeTrue();
        await requestTask;

        using var responseCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        responseCts.CancelAfter(DefaultReceiveTimeout);

        var response = await transport.ReceiveAsync(ReplyQueue, responseCts.Token);
        response.HasMessage.Should().BeTrue();
        response.Message!.CorrelationId.Should().Be(correlationId);
        response.Message.Body.ToArray().Should().Equal(responseBody);

        await transport.AcknowledgeAsync(response.DeliveryTag!, responseCts.Token);
    }

    [Fact]
    public async Task DelayedDelivery_ScheduledAt_DeliversAfterDelay()
    {
        var context = await CreateContextAsync();
        await using var transport = await CreateTransportAsync();

        await transport.CreateTopologyAsync(context.Topology, CancellationToken.None);

        var delay = TimeSpan.FromMilliseconds(200);
        var body = "delayed-message"u8.ToArray();

        using var immediateCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        immediateCts.CancelAfter(TimeSpan.FromMilliseconds(75));

        await transport.SendAsync(new TransportMessage
        {
            Destination = TestQueue,
            Body = body,
            ScheduledAt = DateTimeOffset.UtcNow.Add(delay),
            ContentType = "text/plain",
        }, CancellationToken.None);

        var immediateReceive = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await transport.ReceiveAsync(TestQueue, immediateCts.Token));

        immediateReceive.Should().NotBeNull();

        using var delayedCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        delayedCts.CancelAfter(DefaultReceiveTimeout);

        var receiveResult = await transport.ReceiveAsync(TestQueue, delayedCts.Token);
        receiveResult.HasMessage.Should().BeTrue();
        receiveResult.Message!.Body.ToArray().Should().Equal(body);

        await transport.AcknowledgeAsync(receiveResult.DeliveryTag!, delayedCts.Token);
    }

    [Fact]
    public async Task DeadLetter_OnRejectWithoutRequeue_MovesToDlq()
    {
        var context = await CreateContextAsync();
        await using var transport = await CreateTransportAsync();

        await transport.CreateTopologyAsync(context.Topology, CancellationToken.None);

        var body = "poison"u8.ToArray();
        await transport.SendAsync(new TransportMessage
        {
            Destination = TestQueue,
            Body = body,
            ContentType = "text/plain",
        }, CancellationToken.None);

        using var receiveCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        receiveCts.CancelAfter(DefaultReceiveTimeout);

        var received = await transport.ReceiveAsync(TestQueue, receiveCts.Token);
        received.HasMessage.Should().BeTrue();

        await transport.RejectAsync(received.DeliveryTag!, requeue: false, receiveCts.Token);

        using var dlqCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        dlqCts.CancelAfter(DefaultReceiveTimeout);

        var deadLettered = await transport.ReceiveAsync(TestDlq, dlqCts.Token);
        deadLettered.HasMessage.Should().BeTrue();
        deadLettered.Message!.Body.ToArray().Should().Equal(body);
        deadLettered.Message.Headers.Should().ContainKey("x-eventmesh-dead-letter-reason");

        await transport.AcknowledgeAsync(deadLettered.DeliveryTag!, dlqCts.Token);
    }

    [Fact]
    public async Task Retry_OnRejectWithRequeue_RedeliversMessage()
    {
        var context = await CreateContextAsync();
        await using var transport = await CreateTransportAsync();

        await transport.CreateTopologyAsync(context.Topology, CancellationToken.None);

        var body = "retry-me"u8.ToArray();
        await transport.SendAsync(new TransportMessage
        {
            Destination = TestQueue,
            Body = body,
            ContentType = "text/plain",
        }, CancellationToken.None);

        using var firstCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        firstCts.CancelAfter(DefaultReceiveTimeout);

        var firstReceive = await transport.ReceiveAsync(TestQueue, firstCts.Token);
        firstReceive.HasMessage.Should().BeTrue();
        firstReceive.Message!.DeliveryCount.Should().Be(0);

        await transport.RejectAsync(firstReceive.DeliveryTag!, requeue: true, firstCts.Token);

        using var secondCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        secondCts.CancelAfter(DefaultReceiveTimeout);

        var secondReceive = await transport.ReceiveAsync(TestQueue, secondCts.Token);
        secondReceive.HasMessage.Should().BeTrue();
        secondReceive.Message!.Body.ToArray().Should().Equal(body);
        secondReceive.Message.DeliveryCount.Should().Be(1);

        await transport.AcknowledgeAsync(secondReceive.DeliveryTag!, secondCts.Token);
    }

    protected virtual bool SupportsReplay => true;

    [Fact]
    public async Task Replay_FromStore_RedeliversHistoricalMessages()
    {
        if (!SupportsReplay)
        {
            return;
        }

        var context = await CreateContextAsync();
        await using var transport = await CreateTransportAsync();

        await transport.CreateTopologyAsync(context.Topology, CancellationToken.None);

        var firstBody = "replay-1"u8.ToArray();
        var secondBody = "replay-2"u8.ToArray();

        await transport.SendAsync(new TransportMessage
        {
            Destination = TestQueue,
            Body = firstBody,
            ContentType = "text/plain",
        }, CancellationToken.None);

        await transport.SendAsync(new TransportMessage
        {
            Destination = TestQueue,
            Body = secondBody,
            ContentType = "text/plain",
        }, CancellationToken.None);

        using var drainCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        drainCts.CancelAfter(DefaultReceiveTimeout);

        var first = await transport.ReceiveAsync(TestQueue, drainCts.Token);
        await transport.AcknowledgeAsync(first.DeliveryTag!, drainCts.Token);

        var second = await transport.ReceiveAsync(TestQueue, drainCts.Token);
        await transport.AcknowledgeAsync(second.DeliveryTag!, drainCts.Token);

        var replayed = await ReplayMessagesAsync(transport, new ReplayOptions
        {
            Source = TestQueue,
        }, CancellationToken.None);

        replayed.Should().Be(2);

        using var replayCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
        replayCts.CancelAfter(DefaultReceiveTimeout);

        var replayedFirst = await transport.ReceiveAsync(TestQueue, replayCts.Token);
        replayedFirst.HasMessage.Should().BeTrue();
        replayedFirst.Message!.Headers.Should().ContainKey("x-eventmesh-replay");
        await transport.AcknowledgeAsync(replayedFirst.DeliveryTag!, replayCts.Token);

        var replayedSecond = await transport.ReceiveAsync(TestQueue, replayCts.Token);
        replayedSecond.HasMessage.Should().BeTrue();
        await transport.AcknowledgeAsync(replayedSecond.DeliveryTag!, replayCts.Token);
    }

    [Fact]
    public async Task GetCapabilities_DeclaresExpectedFeatures()
    {
        await using var transport = await CreateTransportAsync();
        var capabilities = transport.GetCapabilities();

        capabilities.SupportsAll(RequiredCapabilities).Should().BeTrue(
            "transport should declare all required capabilities");
    }

    protected virtual BrokerCapabilities RequiredCapabilities =>
        BrokerCapabilities.DelayedDelivery
        | BrokerCapabilities.Priority
        | BrokerCapabilities.Replay
        | BrokerCapabilities.DeadLettering
        | BrokerCapabilities.ConsumerGroups
        | BrokerCapabilities.RequestResponse
        | BrokerCapabilities.PubSub
        | BrokerCapabilities.Queues;

    protected abstract Task<long> ReplayMessagesAsync(
        IBrokerTransport transport,
        ReplayOptions options,
        CancellationToken cancellationToken);

    protected sealed class TransportCompatibilityContext
    {
        public required TopologyDefinition Topology { get; init; }
    }
}
