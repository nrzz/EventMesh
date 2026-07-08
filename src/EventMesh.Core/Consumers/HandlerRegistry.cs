using System.Collections.Concurrent;
using EventMesh.Abstractions.Messaging;

namespace EventMesh.Core.Consumers;

/// <summary>
/// Maps message types to registered handlers.
/// </summary>
public sealed class HandlerRegistry
{
    private readonly ConcurrentDictionary<Type, HandlerRegistration> _handlers = new();

    /// <summary>
    /// Registers a delegate handler for the specified message type.
    /// </summary>
    public void Register<T>(Func<T, CancellationToken, Task> handler) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(handler);
        _handlers[typeof(T)] = new HandlerRegistration(
            typeof(T),
            (message, cancellationToken) => handler((T)message, cancellationToken));
    }

    /// <summary>
    /// Registers a typed handler for the specified message type.
    /// </summary>
    public void Register<T>(IMessageHandler<T> handler) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(handler);
        _handlers[typeof(T)] = new HandlerRegistration(
            typeof(T),
            (message, cancellationToken) => handler.HandleAsync((T)message, cancellationToken));
    }

    /// <summary>
    /// Gets all registered handler message types.
    /// </summary>
    public IReadOnlyCollection<Type> RegisteredTypes => _handlers.Keys.ToArray();

    /// <summary>
    /// Attempts to resolve a handler for the specified message type.
    /// </summary>
    public bool TryGetHandler(Type messageType, out HandlerRegistration? registration) =>
        _handlers.TryGetValue(messageType, out registration);

    /// <summary>
    /// Represents a registered message handler.
    /// </summary>
    public sealed class HandlerRegistration
    {
        public HandlerRegistration(Type messageType, Func<object, CancellationToken, Task> invoker)
        {
            MessageType = messageType;
            Invoker = invoker;
        }

        public Type MessageType { get; }

        public Func<object, CancellationToken, Task> Invoker { get; }
    }
}
