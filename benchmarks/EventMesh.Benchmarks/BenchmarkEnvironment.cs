using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Transport;
using EventMesh.Core;
using EventMesh.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventMesh.Benchmarks;

internal static class BenchmarkEnvironment
{
    public static async Task<(IServiceProvider Provider, IMessageBus Bus)> CreateAsync(
        CancellationToken cancellationToken = default)
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));
        services.AddInMemoryTransport(options =>
        {
            options.ReceivePollInterval = TimeSpan.FromMicroseconds(50);
            options.DelayCheckInterval = TimeSpan.FromMilliseconds(5);
        });

        var factoryProvider = services.BuildServiceProvider();
        var transportFactory = factoryProvider.GetRequiredService<IBrokerTransportFactory>();

        services.AddEventMesh(mesh =>
        {
            mesh.UseTransport(transportFactory);
        });

        var provider = services.BuildServiceProvider();

        foreach (var hostedService in provider.GetServices<IHostedService>())
        {
            await hostedService.StartAsync(cancellationToken);
        }

        var bus = provider.GetRequiredService<IMessageBus>();
        return (provider, bus);
    }

    public static async Task DisposeAsync(IServiceProvider provider)
    {
        foreach (var hostedService in provider.GetServices<IHostedService>().Reverse())
        {
            try
            {
                await hostedService.StopAsync(CancellationToken.None);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        if (provider is IAsyncDisposable asyncDisposable)
        {
            await asyncDisposable.DisposeAsync();
        }
        else if (provider is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}
