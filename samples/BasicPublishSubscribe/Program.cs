using EventMesh.Abstractions.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BasicPublishSubscribe;

public static class Program
{
    public static async Task Main(string[] args)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var host = await SampleHostFactory.CreateAsync(cts.Token);

        var bus = host.Services.GetRequiredService<IMessageBus>();
        var handler = host.Services.GetRequiredService<OrderCreatedHandler>();

        await using var consumer = await bus.SubscribeAsync<OrderCreated>(
            handler.HandleAsync,
            cancellationToken: cts.Token);

        var order = new OrderCreated(Guid.NewGuid(), 42.50m);
        await bus.PublishAsync(order, cancellationToken: cts.Token);

        var received = await handler.WaitForMessageAsync(cts.Token);
        Console.WriteLine($"Received order {received.OrderId} with amount {received.Amount}");
    }
}
