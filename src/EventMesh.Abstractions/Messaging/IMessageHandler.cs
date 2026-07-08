namespace EventMesh.Abstractions.Messaging;

/// <summary>
/// Handles messages of a specific type dispatched by the message bus.
/// </summary>
/// <typeparam name="T">The message payload type.</typeparam>
public interface IMessageHandler<in T> where T : notnull
{
    /// <summary>
    /// Handles the incoming message.
    /// </summary>
    /// <param name="message">The deserialized message payload.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task HandleAsync(T message, CancellationToken cancellationToken = default);
}
