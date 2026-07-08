using EventMesh.Abstractions.Configuration;
using EventMesh.Abstractions.Pipeline;
using EventMesh.Abstractions.Reliability;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace EventMesh.Core.Pipeline;

/// <summary>
/// Persists published messages to the transactional outbox instead of sending immediately.
/// </summary>
public sealed class OutboxFilter<T> : IPublishFilter<T> where T : notnull
{
    private readonly EventMeshOptions _options;
    private readonly IServiceProvider _serviceProvider;

    public OutboxFilter(IOptions<EventMeshOptions> options, IServiceProvider serviceProvider)
    {
        _options = options.Value;
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public async Task FilterAsync(
        PublishContext<T> context,
        FilterDelegate<PublishContext<T>> next,
        CancellationToken cancellationToken = default)
    {
        if (!_options.EnableOutbox)
        {
            await next(context, cancellationToken);
            return;
        }

        var outboxStore = _serviceProvider.GetService<IOutboxStore>()
            ?? throw new InvalidOperationException(
                "Outbox is enabled but no IOutboxStore implementation is registered.");

        if (context.Envelope is null)
        {
            throw new InvalidOperationException("Outbox filter requires a constructed envelope.");
        }

        if (string.IsNullOrWhiteSpace(context.Destination))
        {
            throw new InvalidOperationException("Outbox filter requires a resolved destination.");
        }

        var outboxMessage = new OutboxMessage
        {
            Id = context.Envelope.Id,
            Envelope = context.Envelope,
            Destination = context.Destination,
            CreatedAt = DateTimeOffset.UtcNow,
            State = OutboxMessageState.Pending,
        };

        await outboxStore.AddAsync(outboxMessage, cancellationToken);
    }
}
