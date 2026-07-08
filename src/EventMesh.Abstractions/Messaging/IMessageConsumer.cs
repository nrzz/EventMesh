namespace EventMesh.Abstractions.Messaging;

/// <summary>
/// Represents an active message consumer with lifecycle management.
/// </summary>
public interface IMessageConsumer : IAsyncDisposable
{
    /// <summary>
    /// Gets the unique identifier for this consumer instance.
    /// </summary>
    string ConsumerId { get; }

    /// <summary>
    /// Gets the subscription or queue name being consumed.
    /// </summary>
    string Subscription { get; }

    /// <summary>
    /// Gets a value indicating whether the consumer is actively receiving messages.
    /// </summary>
    bool IsRunning { get; }

    /// <summary>
    /// Gets a value indicating whether the consumer is paused.
    /// </summary>
    bool IsPaused { get; }

    /// <summary>
    /// Starts receiving messages.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops receiving messages and waits for in-flight handlers to complete.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task StopAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Temporarily pauses message delivery without tearing down the subscription.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task PauseAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Resumes message delivery after a prior pause.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task ResumeAsync(CancellationToken cancellationToken = default);
}
