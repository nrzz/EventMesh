namespace EventMesh.Core.Consumers;

using EventMesh.Abstractions.Messaging;

/// <summary>
/// Tracks active message consumers for lifecycle management.
/// </summary>
public interface IConsumerManager
{
    /// <summary>
    /// Adds a consumer to be hosted by <see cref="MessageConsumerHost"/>.
    /// </summary>
    void Add(IMessageConsumer consumer);

    /// <summary>
    /// Gets the currently registered consumers.
    /// </summary>
    IReadOnlyList<IMessageConsumer> Consumers { get; }
}

/// <inheritdoc />
internal sealed class ConsumerManager : IConsumerManager
{
    private readonly List<IMessageConsumer> _consumers = [];
    private readonly object _sync = new();

    public IReadOnlyList<IMessageConsumer> Consumers
    {
        get
        {
            lock (_sync)
            {
                return _consumers.ToArray();
            }
        }
    }

    public void Add(IMessageConsumer consumer)
    {
        ArgumentNullException.ThrowIfNull(consumer);
        lock (_sync)
        {
            _consumers.Add(consumer);
        }
    }
}
