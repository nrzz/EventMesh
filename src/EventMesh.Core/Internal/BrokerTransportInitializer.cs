using EventMesh.Abstractions.Transport;
using EventMesh.Core.Capabilities;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace EventMesh.Core.Internal;

/// <summary>
/// Initializes the broker transport and validates capabilities at startup.
/// </summary>
internal sealed class BrokerTransportInitializer : IHostedService
{
    private readonly BrokerTransportProvider _transportProvider;
    private readonly CapabilityEngine _capabilityEngine;
    private readonly ILogger<BrokerTransportInitializer> _logger;

    public BrokerTransportInitializer(
        BrokerTransportProvider transportProvider,
        CapabilityEngine capabilityEngine,
        ILogger<BrokerTransportInitializer> logger)
    {
        _transportProvider = transportProvider;
        _capabilityEngine = capabilityEngine;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var transport = await _transportProvider.GetTransportAsync(cancellationToken);
        _capabilityEngine.Validate(transport);
        _logger.LogInformation(
            "EventMesh transport '{TransportName}' initialized with capabilities {Capabilities}.",
            transport.Name,
            transport.GetCapabilities());
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
