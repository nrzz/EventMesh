using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Abstractions.Plugins;
using EventMesh.Abstractions.Transport;
using EventMesh.Core.Capabilities;
using EventMesh.Core.Pipeline;
using EventMesh.Plugin.Sdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EventMesh.Core.Configuration;

/// <summary>
/// Fluent builder for configuring EventMesh transports, filters, and plugins.
/// </summary>
public sealed class EventMeshBuilder
{
    private readonly IServiceCollection _services;
    private readonly EventMeshOptions _options;
    private readonly CapabilityEngine _capabilityEngine;
    private readonly List<IEventMeshPlugin> _plugins = [];
    private IBrokerTransportFactory? _transportFactory;
    private Action<IServiceCollection>? _filterConfiguration;
    private bool _built;

    internal EventMeshBuilder(IServiceCollection services, EventMeshOptions options, CapabilityEngine capabilityEngine)
    {
        _services = services;
        _options = options;
        _capabilityEngine = capabilityEngine;
    }

    /// <summary>
    /// Configures root EventMesh options.
    /// </summary>
    public EventMeshBuilder Configure(Action<EventMeshOptions> configure)
    {
        configure(_options);
        return this;
    }

    /// <summary>
    /// Registers a broker transport factory.
    /// </summary>
    public EventMeshBuilder UseTransport(IBrokerTransportFactory transportFactory)
    {
        ArgumentNullException.ThrowIfNull(transportFactory);
        _transportFactory = transportFactory;
        _options.DefaultTransport = transportFactory.TransportName;
        return this;
    }

    /// <summary>
    /// Enables transactional outbox publishing.
    /// </summary>
    public EventMeshBuilder EnableOutbox()
    {
        _options.EnableOutbox = true;
        return this;
    }

    /// <summary>
    /// Enables inbox deduplication.
    /// </summary>
    public EventMeshBuilder EnableInbox()
    {
        _options.EnableInbox = true;
        return this;
    }

    /// <summary>
    /// Enables replay and requires native or emulated replay support.
    /// </summary>
    public EventMeshBuilder EnableReplay()
    {
        _capabilityEngine.Require(BrokerCapabilities.Replay);
        return this;
    }

    /// <summary>
    /// Enables emulated delayed delivery through the in-memory scheduler.
    /// </summary>
    public EventMeshBuilder EnableDelayedDeliveryEmulation()
    {
        _capabilityEngine.EnableEmulation(BrokerCapabilities.DelayedDelivery);
        _capabilityEngine.EnableEmulation(BrokerCapabilities.NativeScheduling);
        return this;
    }

    /// <summary>
    /// Enables emulated dead-letter routing through convention-based destinations.
    /// </summary>
    public EventMeshBuilder EnableDeadLetterEmulation()
    {
        _capabilityEngine.EnableEmulation(BrokerCapabilities.DeadLettering);
        return this;
    }

    /// <summary>
    /// Registers an EventMesh plugin.
    /// </summary>
    public EventMeshBuilder AddPlugin(IEventMeshPlugin plugin)
    {
        ArgumentNullException.ThrowIfNull(plugin);
        _plugins.Add(plugin);
        plugin.Configure(_options);
        return this;
    }

    /// <summary>
    /// Adds custom publish and consume filters to the DI container.
    /// </summary>
    public EventMeshBuilder AddFilters(Action<IFilterRegistration> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var registration = new FilterRegistration(_services);
        configure(registration);
        return this;
    }

    /// <summary>
    /// Replaces the default built-in filter registration.
    /// </summary>
    public EventMeshBuilder ConfigureFilters(Action<IServiceCollection> configure)
    {
        _filterConfiguration = configure;
        return this;
    }

    internal void Build()
    {
        if (_built)
        {
            return;
        }

        foreach (var plugin in _plugins)
        {
            _services.AddSingleton(plugin);
        }

        foreach (var plugin in _plugins)
        {
            if (plugin is PluginBase pluginBase)
            {
                var filterRegistration = new FilterRegistration(_services);
                var pluginBuilder = new PluginBuilder(_services, filterRegistration, _options);
                pluginBase.ConfigurePlugin(pluginBuilder);
            }
        }

        if (_transportFactory is null)
        {
            throw new InvalidOperationException(
                "An IBrokerTransportFactory must be registered via UseTransport(...) before building EventMesh.");
        }

        _services.AddSingleton(_transportFactory);
        _services.AddSingleton(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<EventMeshOptions>>().Value;
            options.TransportSettings.TryGetValue(_transportFactory.TransportName, out var settings);
            IReadOnlyDictionary<string, string>? transportSettings = settings is null
                ? null
                : new Dictionary<string, string>(settings);
            return new Internal.BrokerTransportProvider(_transportFactory, transportSettings);
        });
        _services.AddSingleton<IBrokerTransport, Internal.BrokerTransportAccessor>();
        _services.AddSingleton<IHostedService, Internal.BrokerTransportInitializer>();

        if (_filterConfiguration is not null)
        {
            _filterConfiguration(_services);
        }
        else
        {
            RegisterDefaultFilters(_services);
        }

        _built = true;
    }

    private static void RegisterDefaultFilters(IServiceCollection services)
    {
        services.AddTransient(typeof(IPublishFilter<>), typeof(TracingFilter<>));
        services.AddTransient(typeof(IPublishFilter<>), typeof(SerializationFilter<>));
        services.AddTransient(typeof(IPublishFilter<>), typeof(OutboxFilter<>));
        services.AddTransient(typeof(IPublishFilter<>), typeof(MetricsFilter<>));

        services.AddTransient(typeof(IConsumeFilter<>), typeof(MetricsFilter<>));
        services.AddTransient(typeof(IConsumeFilter<>), typeof(TracingFilter<>));
        services.AddTransient(typeof(IConsumeFilter<>), typeof(SerializationFilter<>));
        services.AddTransient(typeof(IConsumeFilter<>), typeof(RetryFilter<>));
        services.AddTransient(typeof(IConsumeFilter<>), typeof(DeadLetterFilter<>));
    }
}

/// <summary>
/// Registration surface for custom pipeline filters.
/// </summary>
public interface IFilterRegistration
{
    /// <summary>
    /// Registers a publish filter for all message types.
    /// </summary>
    void AddPublishFilter<TFilter>() where TFilter : class;

    /// <summary>
    /// Registers an open-generic publish filter for all message types.
    /// </summary>
    void AddPublishFilter(Type openGenericFilterType);

    /// <summary>
    /// Registers a consume filter for all message types.
    /// </summary>
    void AddConsumeFilter<TFilter>() where TFilter : class;

    /// <summary>
    /// Registers an open-generic consume filter for all message types.
    /// </summary>
    void AddConsumeFilter(Type openGenericFilterType);
}

internal sealed class FilterRegistration : IFilterRegistration
{
    private readonly IServiceCollection _services;

    public FilterRegistration(IServiceCollection services)
    {
        _services = services;
    }

    public void AddPublishFilter<TFilter>() where TFilter : class =>
        _services.AddTransient(typeof(IPublishFilter<>), typeof(TFilter));

    public void AddPublishFilter(Type openGenericFilterType)
    {
        ArgumentNullException.ThrowIfNull(openGenericFilterType);
        _services.AddTransient(typeof(IPublishFilter<>), openGenericFilterType);
    }

    public void AddConsumeFilter<TFilter>() where TFilter : class =>
        _services.AddTransient(typeof(IConsumeFilter<>), typeof(TFilter));

    public void AddConsumeFilter(Type openGenericFilterType)
    {
        ArgumentNullException.ThrowIfNull(openGenericFilterType);
        _services.AddTransient(typeof(IConsumeFilter<>), openGenericFilterType);
    }
}
