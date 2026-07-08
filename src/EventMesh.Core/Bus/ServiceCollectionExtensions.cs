using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Observability;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Abstractions.Scheduling;
using EventMesh.Abstractions.Serialization;
using EventMesh.Abstractions.Transport;
using EventMesh.Core.Bus;
using EventMesh.Core.Capabilities;
using EventMesh.Core.Configuration;
using EventMesh.Core.Consumers;
using EventMesh.Core.Observability;
using EventMesh.Core.Pipeline;
using EventMesh.Core.RequestResponse;
using EventMesh.Core.Scheduling;
using EventMesh.Core.Internal;
using EventMesh.Core.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace EventMesh.Core;

/// <summary>
/// Dependency injection extensions for EventMesh.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds EventMesh core services to the service collection.
    /// </summary>
    public static MessageBusBuilder AddEventMesh(
        this IServiceCollection services,
        Action<EventMeshBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new EventMeshOptions();
        var capabilityEngine = new CapabilityEngine();

        services.TryAddSingleton(capabilityEngine);
        services.TryAddSingleton<CapabilityEmulator>();
        services.TryAddSingleton<ICorrelationContext, CorrelationContext>();
        services.TryAddSingleton<EventMeshMetrics>();
        services.TryAddSingleton<HandlerRegistry>();
        services.TryAddSingleton<IConsumerManager, ConsumerManager>();
        services.TryAddSingleton<RequestResponseManager>();
        services.TryAddSingleton<IFilterPipeline, FilterPipeline>();
        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<MessageTopicResolver>();
        services.TryAddSingleton<InMemoryMessageScheduler>();
        services.TryAddSingleton<IMessageScheduler>(sp => sp.GetRequiredService<InMemoryMessageScheduler>());
        services.TryAddSingleton<IMessageBus, MessageBus>();

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, ScheduledMessageDispatcher>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, MessageConsumerHost>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, HandlerSubscriptionHostedService>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, OutboxDispatcher>());

        services.AddSingleton(options);
        services.AddSingleton<IOptions<EventMeshOptions>>(new OptionsWrapper<EventMeshOptions>(options));

        var meshBuilder = new EventMeshBuilder(services, options, capabilityEngine);
        meshBuilder.EnableDelayedDeliveryEmulation();
        meshBuilder.EnableDeadLetterEmulation();
        configure?.Invoke(meshBuilder);
        meshBuilder.Build();

        if (options.EnableOutbox)
        {
            capabilityEngine.Require(BrokerCapabilities.MessagePersistence);
        }

        return new MessageBusBuilder(services, options, capabilityEngine);
    }
}
