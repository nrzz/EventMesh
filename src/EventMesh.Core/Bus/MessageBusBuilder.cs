using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Messaging;
using EventMesh.Core.Capabilities;
using EventMesh.Core.Configuration;
using EventMesh.Core.Consumers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace EventMesh.Core.Bus;

/// <summary>
/// Builds and configures EventMesh bus services.
/// </summary>
public sealed class MessageBusBuilder
{
    private readonly IServiceCollection _services;
    private readonly EventMeshOptions _options;
    private readonly CapabilityEngine _capabilityEngine;

    internal MessageBusBuilder(
        IServiceCollection services,
        EventMeshOptions options,
        CapabilityEngine capabilityEngine)
    {
        _services = services;
        _options = options;
        _capabilityEngine = capabilityEngine;
    }

    /// <summary>
    /// Configures EventMesh using the fluent <see cref="EventMeshBuilder"/>.
    /// </summary>
    public MessageBusBuilder Configure(Action<EventMeshBuilder> configure)
    {
        var builder = new EventMeshBuilder(_services, _options, _capabilityEngine);
        configure(builder);
        builder.Build();
        return this;
    }

    /// <summary>
    /// Registers a typed message handler in DI and the handler registry.
    /// </summary>
    public MessageBusBuilder AddMessageHandler<THandler>() where THandler : class
    {
        _services.AddSingleton<THandler>();
        _services.AddSingleton<IHostedService, RegisteredHandlerHostedService<THandler>>();
        return this;
    }
}

/// <summary>
/// Registers handlers from DI at application startup.
/// </summary>
internal sealed class RegisteredHandlerHostedService<THandler> : IHostedService
{
    private readonly THandler _handler;
    private readonly HandlerRegistry _handlerRegistry;

    public RegisteredHandlerHostedService(THandler handler, HandlerRegistry handlerRegistry)
    {
        _handler = handler;
        _handlerRegistry = handlerRegistry;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var handlerType = typeof(THandler);
        var messageHandlerInterface = handlerType
            .GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IMessageHandler<>));

        if (messageHandlerInterface is null)
        {
            throw new InvalidOperationException(
                $"Type '{handlerType.FullName}' does not implement IMessageHandler<T>.");
        }

        var messageType = messageHandlerInterface.GetGenericArguments()[0];
        var registerMethod = typeof(HandlerRegistry)
            .GetMethods()
            .First(method => method.Name == nameof(HandlerRegistry.Register) && method.IsGenericMethod)
            .MakeGenericMethod(messageType);

        registerMethod.Invoke(_handlerRegistry, [_handler]);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>
/// Auto-subscribes handlers from <see cref="HandlerRegistry"/> at startup.
/// </summary>
internal sealed class HandlerSubscriptionHostedService : IHostedService
{
    private readonly IMessageBus _messageBus;
    private readonly HandlerRegistry _handlerRegistry;

    public HandlerSubscriptionHostedService(IMessageBus messageBus, HandlerRegistry handlerRegistry)
    {
        _messageBus = messageBus;
        _handlerRegistry = handlerRegistry;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var messageType in _handlerRegistry.RegisteredTypes)
        {
            if (!_handlerRegistry.TryGetHandler(messageType, out var registration) || registration is null)
            {
                continue;
            }

            var subscribeMethod = typeof(HandlerSubscriptionHelper)
                .GetMethod(nameof(HandlerSubscriptionHelper.SubscribeAsync))!
                .MakeGenericMethod(messageType);

            await (Task<IMessageConsumer>)subscribeMethod.Invoke(
                null,
                [_messageBus, registration, cancellationToken])!;
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

internal static class HandlerSubscriptionHelper
{
    public static Task<IMessageConsumer> SubscribeAsync<T>(
        IMessageBus bus,
        HandlerRegistry.HandlerRegistration registration,
        CancellationToken cancellationToken) where T : notnull =>
        bus.SubscribeAsync<T>(
            (message, token) => registration.Invoker(message, token),
            options: null,
            cancellationToken);
}
