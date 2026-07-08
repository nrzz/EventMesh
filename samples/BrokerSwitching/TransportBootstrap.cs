using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Core;
using EventMesh.Transport.InMemory;
using EventMesh.Transport.RabbitMQ;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BrokerSwitching;

internal static class TransportBootstrap
{
    public static void RegisterTransport(IServiceCollection services, IConfiguration configuration)
    {
        var transport = configuration["EventMesh:Transport"] ?? "inmemory";

        switch (transport.ToLowerInvariant())
        {
            case "rabbitmq":
                services.AddRabbitMqTransport();
                break;
            case "inmemory":
            default:
                services.AddInMemoryTransport();
                break;
        }
    }

    public static IBrokerTransportFactory ResolveFactory(IServiceProvider services, IConfiguration configuration)
    {
        var transport = configuration["EventMesh:Transport"] ?? "inmemory";
        return transport.ToLowerInvariant() switch
        {
            "rabbitmq" => services.GetRequiredService<RabbitMqTransportFactory>(),
            _ => services.GetRequiredService<InMemoryTransportFactory>(),
        };
    }
}

internal sealed class DeferredTransportFactory : IBrokerTransportFactory
{
    private readonly Func<IBrokerTransportFactory> _factoryResolver;

    public DeferredTransportFactory(Func<IBrokerTransportFactory> factoryResolver)
    {
        _factoryResolver = factoryResolver;
    }

    public string TransportName => _factoryResolver().TransportName;

    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default) =>
        _factoryResolver().CreateTransportAsync(settings, cancellationToken);
}

public sealed record InventoryUpdated(string Sku, int Quantity);

public sealed class InventoryUpdatedHandler : IMessageHandler<InventoryUpdated>
{
    private readonly TaskCompletionSource<InventoryUpdated> _received = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task<InventoryUpdated> WaitForMessageAsync(CancellationToken cancellationToken) =>
        _received.Task.WaitAsync(cancellationToken);

    public Task HandleAsync(InventoryUpdated message, CancellationToken cancellationToken = default)
    {
        _received.TrySetResult(message);
        return Task.CompletedTask;
    }
}

public static class SampleHostFactory
{
    public static async Task<IHost> CreateAsync(IConfiguration configuration, CancellationToken cancellationToken = default)
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Configuration.AddConfiguration(configuration);
        hostBuilder.Logging.ClearProviders();
        hostBuilder.Logging.AddConsole();

        TransportBootstrap.RegisterTransport(hostBuilder.Services, configuration);
        IHost? builtHost = null;
        hostBuilder.Services.AddEventMesh(mesh =>
        {
            mesh.UseTransport(new DeferredTransportFactory(() =>
                TransportBootstrap.ResolveFactory(builtHost!.Services, configuration)));
        });
        hostBuilder.Services.AddSingleton<InventoryUpdatedHandler>();

        builtHost = hostBuilder.Build();
        await builtHost.StartAsync(cancellationToken);
        return builtHost;
    }
}
