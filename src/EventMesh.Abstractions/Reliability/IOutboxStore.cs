namespace EventMesh.Abstractions.Reliability;

/// <summary>
/// Persists and dispatches messages using the transactional outbox pattern.
/// </summary>
public interface IOutboxStore
{
    /// <summary>
    /// Adds a message to the outbox within the current transactional scope.
    /// </summary>
    Task AddAsync(OutboxMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves pending outbox messages ready for dispatch.
    /// </summary>
    Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int batchSize, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks an outbox message as successfully published.
    /// </summary>
    Task MarkPublishedAsync(string messageId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records a failed publish attempt for an outbox message.
    /// </summary>
    Task MarkFailedAsync(string messageId, string error, CancellationToken cancellationToken = default);

    /// <summary>
    /// Attempts to claim a pending message for processing.
    /// </summary>
    Task<bool> TryClaimAsync(string messageId, CancellationToken cancellationToken = default);
}
