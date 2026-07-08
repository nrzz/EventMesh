namespace EventMesh.Abstractions.Reliability;

/// <summary>
/// Records processed messages to provide idempotent consumption semantics.
/// </summary>
public interface IInboxStore
{
    /// <summary>
    /// Attempts to register a message for processing using the specified idempotency key.
    /// </summary>
    /// <returns><see langword="true"/> if the message is new; otherwise <see langword="false"/>.</returns>
    Task<bool> TryRegisterAsync(InboxMessage message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks an inbox message as successfully processed.
    /// </summary>
    Task MarkProcessedAsync(string idempotencyKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records a failed processing attempt for an inbox message.
    /// </summary>
    Task MarkFailedAsync(string idempotencyKey, string error, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves an inbox message by idempotency key.
    /// </summary>
    Task<InboxMessage?> GetByIdempotencyKeyAsync(string idempotencyKey, CancellationToken cancellationToken = default);
}
