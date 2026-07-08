using EventMesh.Abstractions.Transport;
using Microsoft.Extensions.DependencyInjection;

namespace EventMesh.Transport.Nats;

/// <summary>
/// Creates <see cref="NatsJetStreamBrokerTransport"/> instances.
/// </summary>
public sealed class NatsTransportFactory : IBrokerTransportFactory
{
    private readonly IServiceProvider _serviceProvider;

    public NatsTransportFactory(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public string TransportName => "nats";

    /// <inheritdoc />
    public Task<IBrokerTransport> CreateTransportAsync(
        IReadOnlyDictionary<string, string>? settings = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (settings is not null && settings.Count > 0)
        {
            var options = _serviceProvider.GetRequiredService<Microsoft.Extensions.Options.IOptions<NatsTransportOptions>>().Value;
            ApplySettings(options, settings);
        }

        IBrokerTransport transport = ActivatorUtilities.CreateInstance<NatsJetStreamBrokerTransport>(_serviceProvider);
        return Task.FromResult(transport);
    }

    private static void ApplySettings(NatsTransportOptions options, IReadOnlyDictionary<string, string> settings)
    {
        if (settings.TryGetValue("Url", out var url))
        {
            options.Url = url;
        }

        if (settings.TryGetValue("Token", out var token))
        {
            options.Token = token;
        }

        if (settings.TryGetValue("Username", out var username))
        {
            options.Username = username;
        }

        if (settings.TryGetValue("Password", out var password))
        {
            options.Password = password;
        }

        if (settings.TryGetValue("CredentialsFile", out var credentialsFile))
        {
            options.CredentialsFile = credentialsFile;
        }

        if (settings.TryGetValue("ConsumerPrefix", out var consumerPrefix))
        {
            options.ConsumerPrefix = consumerPrefix;
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
