using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.InMemory;

/// <summary>
/// Creates <see cref="InMemoryBrokerTransport"/> instances.
/// </summary>
public sealed class InMemoryTransportFactory : IBrokerTransportFactory
{
    private readonly IServiceProvider _serviceProvider;

    public InMemoryTransportFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public string TransportName => "inmemory";

    /// <inheritdoc />
    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (settings is not null && settings.Count > 0)
        {
            var options = _serviceProvider.GetRequiredService<IOptions<InMemoryTransportOptions>>().Value;
            ApplySettings(options, settings);
        }

        IBrokerTransport transport = ActivatorUtilities.CreateInstance<InMemoryBrokerTransport>(_serviceProvider);
        return Task.FromResult(transport);
    }

    private static void ApplySettings(InMemoryTransportOptions options, IReadOnlyDictionary<string, string> settings)
    {
        if (settings.TryGetValue("ReceivePollInterval", out var receivePollInterval)
            && TimeSpan.TryParse(receivePollInterval, out var pollInterval))
        {
            options.ReceivePollInterval = pollInterval;
        }

        if (settings.TryGetValue("DelayCheckInterval", out var delayCheckInterval)
            && TimeSpan.TryParse(delayCheckInterval, out var delayInterval))
        {
            options.DelayCheckInterval = delayInterval;
        }

        if (settings.TryGetValue("MaxPriority", out var maxPriority)
            && int.TryParse(maxPriority, out var priority))
        {
            options.MaxPriority = priority;
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
