using Dapper;
using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Messaging;
using EventMesh.Abstractions.Scheduling;
using EventMesh.Abstractions.Serialization;
using EventMesh.Storage.PostgreSql.Internal;
using EventMesh.Storage.PostgreSql.Schema;
using Npgsql;
using static EventMesh.Storage.PostgreSql.Internal.MessageEnvelopeJson;

namespace EventMesh.Storage.PostgreSql.Scheduling;

/// <summary>
/// PostgreSQL-backed implementation of <see cref="IMessageScheduler"/> for delayed delivery.
/// </summary>
public sealed class PostgreSqlMessageScheduler : IMessageScheduler
{
    private const string ScheduleGroupHeader = "eventmesh.schedule-group-id";

    private readonly NpgsqlDataSource _dataSource;
    private readonly SchemaInitializer _schemaInitializer;
    private readonly IMessageSerializer _serializer;

    public PostgreSqlMessageScheduler(
        NpgsqlDataSource dataSource,
        SchemaInitializer schemaInitializer,
        IMessageSerializer serializer)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _schemaInitializer = schemaInitializer ?? throw new ArgumentNullException(nameof(schemaInitializer));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    /// <inheritdoc />
    public Task<string> ScheduleAsync<T>(
        T message,
        DateTimeOffset scheduledAt,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull
    {
        return ScheduleInternalAsync(message, scheduledAt, options, cancellationToken);
    }

    /// <inheritdoc />
    public Task<string> ScheduleAsync<T>(
        T message,
        TimeSpan delay,
        ScheduleOptions? options = null,
        CancellationToken cancellationToken = default) where T : notnull
    {
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Delay cannot be negative.");
        }

        return ScheduleInternalAsync(message, DateTimeOffset.UtcNow.Add(delay), options, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> CancelAsync(string scheduleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            UPDATE eventmesh_scheduled_messages
            SET status = @CancelledStatus
            WHERE id = @ScheduleId
              AND status = @PendingStatus
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var affected = await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                ScheduleId = scheduleId,
                PendingStatus = (short)ScheduledMessageState.Pending,
                CancelledStatus = (short)ScheduledMessageState.Cancelled,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        return affected > 0;
    }

    /// <inheritdoc />
    public async Task<int> CancelGroupAsync(string scheduleGroupId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleGroupId);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            UPDATE eventmesh_scheduled_messages
            SET status = @CancelledStatus
            WHERE status = @PendingStatus
              AND envelope_json ->> 'scheduleGroupId' = @ScheduleGroupId
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                ScheduleGroupId = scheduleGroupId,
                PendingStatus = (short)ScheduledMessageState.Pending,
                CancelledStatus = (short)ScheduledMessageState.Cancelled,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<ScheduledMessage?> GetAsync(string scheduleId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scheduleId);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            SELECT
                id AS ScheduleId,
                envelope_json::text AS EnvelopeJson,
                destination AS Destination,
                scheduled_at AS ScheduledAt,
                status AS Status,
                created_at AS CreatedAt
            FROM eventmesh_scheduled_messages
            WHERE id = @ScheduleId
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var row = await connection.QuerySingleOrDefaultAsync<ScheduledRow>(new CommandDefinition(
            sql,
            new { ScheduleId = scheduleId },
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        return row is null ? null : MapRow(row);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ScheduledMessage>> GetDueAsync(
        DateTimeOffset asOf,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be greater than zero.");
        }

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            SELECT
                id AS ScheduleId,
                envelope_json::text AS EnvelopeJson,
                destination AS Destination,
                scheduled_at AS ScheduledAt,
                status AS Status,
                created_at AS CreatedAt
            FROM eventmesh_scheduled_messages
            WHERE status = @PendingStatus
              AND scheduled_at <= @AsOf
            ORDER BY scheduled_at
            LIMIT @BatchSize
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var rows = await connection.QueryAsync<ScheduledRow>(new CommandDefinition(
            sql,
            new
            {
                PendingStatus = (short)ScheduledMessageState.Pending,
                AsOf = asOf.UtcDateTime,
                BatchSize = batchSize,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        return rows.Select(MapRow).ToArray();
    }

    private async Task<string> ScheduleInternalAsync<T>(
        T message,
        DateTimeOffset scheduledAt,
        ScheduleOptions? options,
        CancellationToken cancellationToken) where T : notnull
    {
        ArgumentNullException.ThrowIfNull(message);

        var effectiveOptions = options ?? new ScheduleOptions();
        var destination = ResolveDestination(effectiveOptions);
        var scheduleId = string.IsNullOrWhiteSpace(effectiveOptions.ScheduleId)
            ? Guid.NewGuid().ToString("N")
            : effectiveOptions.ScheduleId;

        var contentType = effectiveOptions.PublishOptions?.ContentType ?? _serializer.DefaultContentType;
        var payload = await _serializer.SerializeAsync(message, contentType, cancellationToken).ConfigureAwait(false);

        var headers = effectiveOptions.Headers is null
            ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, string>(effectiveOptions.Headers, StringComparer.OrdinalIgnoreCase);

        if (!string.IsNullOrWhiteSpace(effectiveOptions.ScheduleGroupId))
        {
            headers[ScheduleGroupHeader] = effectiveOptions.ScheduleGroupId;
        }

        var envelope = MessageEnvelope.Create()
            .WithType(typeof(T).FullName ?? typeof(T).Name)
            .WithSource("eventmesh/scheduler")
            .WithDataContentType(contentType)
            .WithData(payload)
            .WithCorrelationId(effectiveOptions.CorrelationId)
            .WithCausationId(effectiveOptions.CausationId)
            .WithHeaders(headers)
            .Build();

        var payloadJson = SerializeScheduledPayload(new ScheduledMessagePayload
        {
            Envelope = envelope,
            ScheduleGroupId = effectiveOptions.ScheduleGroupId,
            Options = SerializeOptions(effectiveOptions),
        });

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            INSERT INTO eventmesh_scheduled_messages
                (id, envelope_json, destination, scheduled_at, status, created_at)
            VALUES
                (@Id, @EnvelopeJson::jsonb, @Destination, @ScheduledAt, @Status, @CreatedAt)
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                Id = scheduleId,
                EnvelopeJson = payloadJson,
                Destination = destination,
                ScheduledAt = scheduledAt.UtcDateTime,
                Status = (short)ScheduledMessageState.Pending,
                CreatedAt = DateTime.UtcNow,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        return scheduleId;
    }

    private static string ResolveDestination(ScheduleOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.Topic))
        {
            return options.Topic;
        }

        if (!string.IsNullOrWhiteSpace(options.RoutingKey))
        {
            return options.RoutingKey;
        }

        throw new InvalidOperationException("ScheduleOptions.Topic or ScheduleOptions.RoutingKey must be specified.");
    }

    private static Dictionary<string, string>? SerializeOptions(ScheduleOptions options)
    {
        if (options.RoutingKey is null
            && options.TimeZoneId is null
            && options.PublishOptions is null)
        {
            return null;
        }

        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (!string.IsNullOrWhiteSpace(options.RoutingKey))
        {
            result["routingKey"] = options.RoutingKey;
        }

        if (!string.IsNullOrWhiteSpace(options.TimeZoneId))
        {
            result["timeZoneId"] = options.TimeZoneId;
        }

        if (options.PublishOptions?.ContentType is not null)
        {
            result["contentType"] = options.PublishOptions.ContentType;
        }

        return result.Count > 0 ? result : null;
    }

    private static ScheduledMessage MapRow(ScheduledRow row)
    {
        var payload = DeserializeScheduledPayload(row.EnvelopeJson);
        var options = DeserializeScheduleOptions(payload, row.Destination);

        return new ScheduledMessage
        {
            ScheduleId = row.ScheduleId,
            Envelope = payload.Envelope,
            Destination = row.Destination,
            ScheduledAt = new DateTimeOffset(DateTime.SpecifyKind(row.ScheduledAt, DateTimeKind.Utc)),
            CreatedAt = new DateTimeOffset(DateTime.SpecifyKind(row.CreatedAt, DateTimeKind.Utc)),
            ScheduleGroupId = payload.ScheduleGroupId,
            Options = options,
            State = (ScheduledMessageState)row.Status,
        };
    }

    private static ScheduleOptions? DeserializeScheduleOptions(ScheduledMessagePayload payload, string destination)
    {
        if (payload.Options is null || payload.Options.Count == 0)
        {
            return payload.ScheduleGroupId is null
                ? new ScheduleOptions { Topic = destination }
                : new ScheduleOptions { Topic = destination, ScheduleGroupId = payload.ScheduleGroupId };
        }

        payload.Options.TryGetValue("routingKey", out var routingKey);
        payload.Options.TryGetValue("timeZoneId", out var timeZoneId);
        payload.Options.TryGetValue("contentType", out var contentType);

        return new ScheduleOptions
        {
            Topic = destination,
            RoutingKey = routingKey,
            TimeZoneId = timeZoneId,
            ScheduleGroupId = payload.ScheduleGroupId,
            CorrelationId = payload.Envelope.CorrelationId,
            CausationId = payload.Envelope.CausationId,
            Headers = payload.Envelope.Headers.Count > 0
                ? new Dictionary<string, string>(payload.Envelope.Headers, StringComparer.OrdinalIgnoreCase)
                : null,
            PublishOptions = contentType is null
                ? null
                : new PublishOptions { ContentType = contentType },
        };
    }

    private sealed class ScheduledRow
    {
        public required string ScheduleId { get; init; }

        public required string EnvelopeJson { get; init; }

        public required string Destination { get; init; }

        public DateTime ScheduledAt { get; init; }

        public short Status { get; init; }

        public DateTime CreatedAt { get; init; }
    }
}
