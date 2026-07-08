using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.AzureServiceBus;

/// <summary>
/// Creates <see cref="AzureServiceBusBrokerTransport"/> instances.
/// </summary>
public sealed class AzureServiceBusTransportFactory : IBrokerTransportFactory
{
    private readonly IServiceProvider _serviceProvider;

    public AzureServiceBusTransportFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public string TransportName => "azureservicebus";

    /// <inheritdoc />
    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (settings is not null && settings.Count > 0)
        {
            var options = _serviceProvider.GetRequiredService<IOptions<AzureServiceBusTransportOptions>>().Value;
            ApplySettings(options, settings);

            if (settings.TryGetValue("ConnectionString", out var connectionString)
                && !string.IsNullOrWhiteSpace(connectionString))
            {
                var connection = _serviceProvider.GetRequiredService<AzureServiceBusConnection>();
                connection.UpdateConnectionString(connectionString);
            }
        }

        IBrokerTransport transport = ActivatorUtilities.CreateInstance<AzureServiceBusBrokerTransport>(_serviceProvider);
        return Task.FromResult(transport);
    }

    private static void ApplySettings(
        AzureServiceBusTransportOptions options,
        IReadOnlyDictionary<string, string> settings)
    {
        if (settings.TryGetValue("ConnectionString", out var connectionString)
            && !string.IsNullOrWhiteSpace(connectionString))
        {
            options.ConnectionString = connectionString;
        }

        if (settings.TryGetValue("PrefetchCount", out var prefetchCount)
            && int.TryParse(prefetchCount, out var parsedPrefetch))
        {
            options.PrefetchCount = parsedPrefetch;
        }

        if (settings.TryGetValue("MaxConcurrentCalls", out var maxConcurrentCalls)
            && int.TryParse(maxConcurrentCalls, out var parsedMaxConcurrentCalls))
        {
            options.MaxConcurrentCalls = parsedMaxConcurrentCalls;
        }

        if (settings.TryGetValue("ReceiveWaitTime", out var receiveWaitTime)
            && TimeSpan.TryParse(receiveWaitTime, out var parsedReceiveWaitTime))
        {
            options.ReceiveWaitTime = parsedReceiveWaitTime;
        }

        if (settings.TryGetValue("ReceivePollInterval", out var receivePollInterval)
            && TimeSpan.TryParse(receivePollInterval, out var parsedReceivePollInterval))
        {
            options.ReceivePollInterval = parsedReceivePollInterval;
        }

        if (settings.TryGetValue("DefaultDeadLetterSuffix", out var deadLetterSuffix))
        {
            options.DefaultDeadLetterSuffix = deadLetterSuffix;
        }

        if (settings.TryGetValue("DefaultMaxDeliveryAttempts", out var maxDeliveryAttempts)
            && int.TryParse(maxDeliveryAttempts, out var attempts))
        {
            options.DefaultMaxDeliveryAttempts = attempts;
        }
    }
}
