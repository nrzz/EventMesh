using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventMesh.Transport.RabbitMQ;

/// <summary>
/// Creates configured <see cref="RabbitMqBrokerTransport"/> instances.
/// </summary>
public sealed class RabbitMqTransportFactory : IBrokerTransportFactory
{
    private readonly IServiceProvider _serviceProvider;

    public RabbitMqTransportFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public string TransportName => "rabbitmq";

    /// <inheritdoc />
    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (settings is not null && settings.Count > 0)
        {
            var options = _serviceProvider.GetRequiredService<IOptions<RabbitMqTransportOptions>>().Value;
            ApplySettings(options, settings);
        }

        IBrokerTransport transport = ActivatorUtilities.CreateInstance<RabbitMqBrokerTransport>(_serviceProvider);
        return Task.FromResult(transport);
    }

    private static void ApplySettings(RabbitMqTransportOptions options, IReadOnlyDictionary<string, string> settings)
    {
        if (settings.TryGetValue("HostName", out var hostName) && !string.IsNullOrWhiteSpace(hostName))
        {
            options.HostName = hostName;
        }

        if (settings.TryGetValue("Port", out var port) && int.TryParse(port, out var parsedPort))
        {
            options.Port = parsedPort;
        }

        if (settings.TryGetValue("UserName", out var userName) && !string.IsNullOrWhiteSpace(userName))
        {
            options.UserName = userName;
        }

        if (settings.TryGetValue("Password", out var password))
        {
            options.Password = password;
        }

        if (settings.TryGetValue("VirtualHost", out var virtualHost) && !string.IsNullOrWhiteSpace(virtualHost))
        {
            options.VirtualHost = virtualHost;
        }

        if (settings.TryGetValue("PrefetchCount", out var prefetchCount)
            && ushort.TryParse(prefetchCount, out var parsedPrefetch))
        {
            options.PrefetchCount = parsedPrefetch;
        }

        if (settings.TryGetValue("PublisherConfirmsEnabled", out var publisherConfirms)
            && bool.TryParse(publisherConfirms, out var confirmsEnabled))
        {
            options.PublisherConfirmsEnabled = confirmsEnabled;
        }

        if (settings.TryGetValue("PublisherConfirmTimeout", out var confirmTimeout)
            && TimeSpan.TryParse(confirmTimeout, out var parsedTimeout))
        {
            options.PublisherConfirmTimeout = parsedTimeout;
        }
    }
}
