using Amazon.SQS;
using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.AmazonSqs;

/// <summary>
/// Dependency injection extensions for the Amazon SQS transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Amazon SQS broker transport and its supporting services.
    /// </summary>
    public static IServiceCollection AddAmazonSqsTransport(
        this IServiceCollection services,
        Action<AmazonSqsTransportOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions<AmazonSqsTransportOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<IAmazonSQS>(static provider =>
        {
            var options = provider.GetRequiredService<IOptions<AmazonSqsTransportOptions>>().Value;
            return options.CreateClient();
        });

        services.TryAddSingleton<AmazonSqsTopologyManager>();
        services.TryAddSingleton<IBrokerTransportFactory, AmazonSqsTransportFactory>();
        services.TryAddSingleton<IBrokerTransport, AmazonSqsBrokerTransport>();

        return services;
    }
}
