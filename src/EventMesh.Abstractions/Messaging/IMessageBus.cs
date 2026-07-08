namespace EventMesh.Abstractions.Messaging;

/// <summary>
/// The primary application-facing API for publishing, scheduling, requesting, replaying, and subscribing to messages.
/// </summary>
public interface IMessageBus
{
    /// <summary>
    /// Publishes a message to the mesh.
    /// </summary>
    /// <typeparam name="T">The message payload type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="options">Optional publish settings.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task PublishAsync<T>(
        T message,
        PublishOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull;

    /// <summary>
    /// Schedules a message for delivery at a specific point in time.
    /// </summary>
    /// <typeparam name="T">The message payload type.</typeparam>
    /// <param name="message">The message to schedule.</param>
    /// <param name="scheduledAt">The UTC time at which the message should be delivered.</param>
    /// <param name="options">Optional schedule settings.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The identifier assigned to the scheduled message.</returns>
    Task<string> ScheduleAsync<T>(
        T message,
        DateTimeOffset scheduledAt,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull;

    /// <summary>
    /// Schedules a message for delivery after a relative delay.
    /// </summary>
    /// <typeparam name="T">The message payload type.</typeparam>
    /// <param name="message">The message to schedule.</param>
    /// <param name="delay">The delay before the message is delivered.</param>
    /// <param name="options">Optional schedule settings.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The identifier assigned to the scheduled message.</returns>
    Task<string> ScheduleAsync<T>(
        T message,
        TimeSpan delay,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull;

    /// <summary>
    /// Sends a request message and waits for a correlated response.
    /// </summary>
    /// <typeparam name="TRequest">The request payload type.</typeparam>
    /// <typeparam name="TResponse">The expected response payload type.</typeparam>
    /// <param name="request">The request message.</param>
    /// <param name="options">Optional request settings.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The deserialized response message.</returns>
    Task<TResponse> RequestAsync<TRequest, TResponse>(
        TRequest request,
        RequestOptions? options = null,
        CancellationToken cancellationToken = default)
        where TRequest : notnull
        where TResponse : notnull;

    /// <summary>
    /// Replays messages from a topic, stream, or archive.
    /// </summary>
    /// <param name="topicOrStream">The source topic, stream, or archive name.</param>
    /// <param name="options">Optional replay settings.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>The number of messages replayed, when available.</returns>
    Task<long> ReplayAsync(
        string topicOrStream,
        ReplayOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to messages of the specified type using a delegate handler.
    /// </summary>
    /// <typeparam name="T">The message payload type.</typeparam>
    /// <param name="handler">The handler invoked for each message.</param>
    /// <param name="options">Optional subscription settings.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A consumer instance that manages the subscription lifecycle.</returns>
    Task<IMessageConsumer> SubscribeAsync<T>(
        Func<T, CancellationToken, Task> handler,
        SubscribeOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull;

    /// <summary>
    /// Subscribes to messages of the specified type using a typed handler.
    /// </summary>
    /// <typeparam name="T">The message payload type.</typeparam>
    /// <param name="handler">The handler invoked for each message.</param>
    /// <param name="options">Optional subscription settings.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    /// <returns>A consumer instance that manages the subscription lifecycle.</returns>
    Task<IMessageConsumer> SubscribeAsync<T>(
        IMessageHandler<T> handler,
        SubscribeOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull;
}
