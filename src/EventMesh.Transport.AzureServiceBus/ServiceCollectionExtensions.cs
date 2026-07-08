using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventMesh.Transport.AzureServiceBus;

/// <summary>
/// Dependency injection extensions for the Azure Service Bus transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Azure Service Bus broker transport and its supporting services.
    /// </summary>
    public static IServiceCollection AddAzureServiceBusTransport(
        this IServiceCollection services,
        Action<AzureServiceBusTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<AzureServiceBusTransportOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<AzureServiceBusConnection>();
        services.TryAddSingleton<AzureServiceBusTopologyManager>();
        services.TryAddSingleton<IBrokerTransportFactory, AzureServiceBusTransportFactory>();
        services.TryAddSingleton<IBrokerTransport, AzureServiceBusBrokerTransport>();

        return services;
    }
}
