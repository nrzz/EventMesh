using EventMesh.Abstractions.Messaging;

namespace EventMesh.Abstractions.Scheduling;

/// <summary>
/// Schedules messages for future delivery and manages scheduled message lifecycle.
/// </summary>
public interface IMessageScheduler
{
    /// <summary>
    /// Schedules a message for delivery at a specific point in time.
    /// </summary>
    Task<string> ScheduleAsync<T>(
        T message,
        DateTimeOffset scheduledAt,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull;

    /// <summary>
    /// Schedules a message for delivery after a relative delay.
    /// </summary>
    Task<string> ScheduleAsync<T>(
        T message,
        TimeSpan delay,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull;

    /// <summary>
    /// Cancels a previously scheduled message.
    /// </summary>
    Task<bool> CancelAsync(string scheduleId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cancels all scheduled messages in the specified schedule group.
    /// </summary>
    Task<int> CancelGroupAsync(string scheduleGroupId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a scheduled message by identifier.
    /// </summary>
    Task<ScheduledMessage?> GetAsync(string scheduleId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves scheduled messages that are due for delivery.
    /// </summary>
    Task<IReadOnlyList<ScheduledMessage>> GetDueAsync(
        DateTimeOffset asOf,
        int batchSize,
        CancellationToken cancellationToken = default);
}
