using EventMesh.Abstractions.Messaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BrokerSwitching;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false)
            .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("EVENTMESH_TRANSPORT")}.json", optional: true)
            .AddEnvironmentVariables()
            .Build();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var host = await SampleHostFactory.CreateAsync(configuration, cts.Token);

        var bus = host.Services.GetRequiredService<IMessageBus>();
        var handler = host.Services.GetRequiredService<InventoryUpdatedHandler>();
        var transportName = configuration["EventMesh:Transport"] ?? "inmemory";

        await using var consumer = await bus.SubscribeAsync<InventoryUpdated>(
            handler.HandleAsync,
            cancellationToken: cts.Token);

        var update = new InventoryUpdated("SKU-100", 25);
        await bus.PublishAsync(update, cancellationToken: cts.Token);

        var received = await handler.WaitForMessageAsync(cts.Token);
        Console.WriteLine(
            $"Transport '{transportName}' delivered inventory update for {received.Sku} ({received.Quantity}).");
    }
}
