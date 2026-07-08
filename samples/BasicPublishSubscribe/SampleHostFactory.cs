using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Core;
using EventMesh.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace BasicPublishSubscribe;

internal static class SampleHostFactory
{
    public static async Task<IHost> CreateAsync(CancellationToken cancellationToken = default)
    {
        var hostBuilder = Host.CreateApplicationBuilder();
        hostBuilder.Logging.ClearProviders();
        hostBuilder.Logging.AddConsole();

        hostBuilder.Services.AddInMemoryTransport();
        IHost? builtHost = null;
        hostBuilder.Services.AddEventMesh(mesh =>
        {
            mesh.UseTransport(new DeferredTransportFactory(
                () => builtHost!.Services.GetRequiredService<InMemoryTransportFactory>(),
                "inmemory"));
        });
        hostBuilder.Services.AddSingleton<OrderCreatedHandler>();

        builtHost = hostBuilder.Build();
        await builtHost.StartAsync(cancellationToken);
        return builtHost;
    }
}

internal sealed class DeferredTransportFactory : IBrokerTransportFactory
{
    private readonly Func<IBrokerTransportFactory> _factoryResolver;
    private readonly string _transportName;

    public DeferredTransportFactory(Func<IBrokerTransportFactory> factoryResolver, string transportName)
    {
        _factoryResolver = factoryResolver;
        _transportName = transportName;
    }

    public string TransportName => _transportName;

    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default) =>
        _factoryResolver().CreateTransportAsync(settings, cancellationToken);
}

public sealed record OrderCreated(Guid OrderId, decimal Amount);

public sealed class OrderCreatedHandler : IMessageHandler<OrderCreated>
{
    private readonly TaskCompletionSource<OrderCreated> _received = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task<OrderCreated> WaitForMessageAsync(CancellationToken cancellationToken) =>
        _received.Task.WaitAsync(cancellationToken);

    public Task HandleAsync(OrderCreated message, CancellationToken cancellationToken = default)
    {
        _received.TrySetResult(message);
        return Task.CompletedTask;
    }
}
