using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.AmazonSqs;

/// <summary>
/// Creates configured <see cref="AmazonSqsBrokerTransport"/> instances.
/// </summary>
public sealed class AmazonSqsTransportFactory : IBrokerTransportFactory
{
    private readonly IServiceProvider _serviceProvider;

    public AmazonSqsTransportFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public string TransportName => "amazonsqs";

    /// <inheritdoc />
    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (settings is not null && settings.Count > 0)
        {
            var options = _serviceProvider.GetRequiredService<IOptions<AmazonSqsTransportOptions>>().Value;
            ApplySettings(options, settings);
        }

        IBrokerTransport transport = ActivatorUtilities.CreateInstance<AmazonSqsBrokerTransport>(_serviceProvider);
        return Task.FromResult(transport);
    }

    private static void ApplySettings(AmazonSqsTransportOptions options, IReadOnlyDictionary<string, string> settings)
    {
        if (settings.TryGetValue("Region", out var region) && !string.IsNullOrWhiteSpace(region))
        {
            options.Region = region;
        }

        if (settings.TryGetValue("ServiceUrl", out var serviceUrl) && !string.IsNullOrWhiteSpace(serviceUrl))
        {
            options.ServiceUrl = serviceUrl;
        }

        if (settings.TryGetValue("AccessKeyId", out var accessKeyId) && !string.IsNullOrWhiteSpace(accessKeyId))
        {
            options.AccessKeyId = accessKeyId;
        }

        if (settings.TryGetValue("SecretAccessKey", out var secretAccessKey))
        {
            options.SecretAccessKey = secretAccessKey;
        }

        if (settings.TryGetValue("SessionToken", out var sessionToken))
        {
            options.SessionToken = sessionToken;
        }

        if (settings.TryGetValue("AccountId", out var accountId) && !string.IsNullOrWhiteSpace(accountId))
        {
            options.AccountId = accountId;
        }

        if (settings.TryGetValue("VisibilityTimeoutSeconds", out var visibilityTimeout)
            && int.TryParse(visibilityTimeout, out var parsedVisibilityTimeout))
        {
            options.VisibilityTimeoutSeconds = parsedVisibilityTimeout;
        }

        if (settings.TryGetValue("DefaultMaxReceiveCount", out var maxReceiveCount)
            && int.TryParse(maxReceiveCount, out var parsedMaxReceiveCount))
        {
            options.DefaultMaxReceiveCount = parsedMaxReceiveCount;
        }
    }
}
