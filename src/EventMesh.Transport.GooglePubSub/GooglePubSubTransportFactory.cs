using EventMesh.Abstractions.Transport;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.GooglePubSub;

/// <summary>
/// Creates <see cref="GooglePubSubBrokerTransport"/> instances.
/// </summary>
public sealed class GooglePubSubTransportFactory : IBrokerTransportFactory
{
    private readonly IServiceProvider _serviceProvider;

    public GooglePubSubTransportFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public string TransportName => "googlepubsub";

    /// <inheritdoc />
    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (settings is not null && settings.Count > 0)
        {
            var options = _serviceProvider.GetRequiredService<IOptions<GooglePubSubTransportOptions>>().Value;
            ApplySettings(options, settings);
        }

        IBrokerTransport transport = new GooglePubSubBrokerTransport(
            _serviceProvider.GetRequiredService<GooglePubSubTopologyManager>(),
            _serviceProvider.GetRequiredService<IOptions<GooglePubSubTransportOptions>>(),
            _serviceProvider.GetRequiredService<ILogger<GooglePubSubBrokerTransport>>());
        return Task.FromResult(transport);
    }

    private static void ApplySettings(GooglePubSubTransportOptions options, IReadOnlyDictionary<string, string> settings)
    {
        if (settings.TryGetValue("ProjectId", out var projectId))
        {
            options.ProjectId = projectId;
        }

        if (settings.TryGetValue("EmulatorDetection", out var emulatorDetection)
            && Enum.TryParse<EmulatorDetection>(emulatorDetection, ignoreCase: true, out var parsedDetection))
        {
            options.EmulatorDetection = parsedDetection;
        }

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

        if (settings.TryGetValue("DefaultDeadLetterSuffix", out var deadLetterSuffix))
        {
            options.DefaultDeadLetterSuffix = deadLetterSuffix;
        }

        if (settings.TryGetValue("DefaultMaxDeliveryAttempts", out var maxDeliveryAttempts)
            && int.TryParse(maxDeliveryAttempts, out var attempts))
        {
            options.DefaultMaxDeliveryAttempts = attempts;
        }

        if (settings.TryGetValue("AckDeadlineSeconds", out var ackDeadlineSeconds)
            && int.TryParse(ackDeadlineSeconds, out var deadline))
        {
            options.AckDeadlineSeconds = deadline;
        }

        if (settings.TryGetValue("EnableMessageOrdering", out var enableMessageOrdering)
            && bool.TryParse(enableMessageOrdering, out var orderingEnabled))
        {
            options.EnableMessageOrdering = orderingEnabled;
        }
    }
}
