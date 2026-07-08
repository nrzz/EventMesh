using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventMesh.Transport.GooglePubSub;

/// <summary>
/// Dependency injection extensions for the Google Pub/Sub transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Google Pub/Sub broker transport and its supporting services.
    /// </summary>
    public static IServiceCollection AddGooglePubSubTransport(
        this IServiceCollection services,
        Action<GooglePubSubTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<GooglePubSubTransportOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<GooglePubSubTopologyManager>();
        services.TryAddSingleton<IBrokerTransportFactory, GooglePubSubTransportFactory>();
        services.TryAddSingleton<IBrokerTransport, GooglePubSubBrokerTransport>();

        return services;
    }
}
