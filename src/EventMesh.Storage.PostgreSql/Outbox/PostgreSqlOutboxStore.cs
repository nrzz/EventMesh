using Dapper;
using EventMesh.Abstractions.Reliability;
using EventMesh.Storage.PostgreSql.Internal;
using EventMesh.Storage.PostgreSql.Schema;
using Npgsql;

namespace EventMesh.Storage.PostgreSql.Outbox;

/// <summary>
/// PostgreSQL-backed implementation of <see cref="IOutboxStore"/>.
/// </summary>
public sealed class PostgreSqlOutboxStore : IOutboxStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly SchemaInitializer _schemaInitializer;

    public PostgreSqlOutboxStore(NpgsqlDataSource dataSource, SchemaInitializer schemaInitializer)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _schemaInitializer = schemaInitializer ?? throw new ArgumentNullException(nameof(schemaInitializer));
    }

    /// <inheritdoc />
    public async Task AddAsync(OutboxMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(message.Envelope);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            INSERT INTO eventmesh_outbox
                (id, message_id, envelope_json, destination, created_at, processed_at, status)
            VALUES
                (@Id, @MessageId, @EnvelopeJson::jsonb, @Destination, @CreatedAt, @ProcessedAt, @Status)
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                message.Id,
                MessageId = message.Envelope.Id,
                EnvelopeJson = MessageEnvelopeJson.Serialize(message.Envelope),
                message.Destination,
                CreatedAt = message.CreatedAt.UtcDateTime,
                ProcessedAt = message.PublishedAt?.UtcDateTime,
                Status = (short)message.State,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<OutboxMessage>> GetPendingAsync(int batchSize, CancellationToken cancellationToken = default)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), batchSize, "Batch size must be greater than zero.");
        }

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            SELECT
                id AS Id,
                message_id AS MessageId,
                envelope_json::text AS EnvelopeJson,
                destination AS Destination,
                created_at AS CreatedAt,
                processed_at AS ProcessedAt,
                status AS Status
            FROM eventmesh_outbox
            WHERE status = @PendingStatus
            ORDER BY created_at
            LIMIT @BatchSize
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var rows = await connection.QueryAsync<OutboxRow>(new CommandDefinition(
            sql,
            new
            {
                PendingStatus = (short)OutboxMessageState.Pending,
                BatchSize = batchSize,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        return rows.Select(MapRow).ToArray();
    }

    /// <inheritdoc />
    public async Task MarkPublishedAsync(string messageId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            UPDATE eventmesh_outbox
            SET status = @Status,
                processed_at = @ProcessedAt
            WHERE message_id = @MessageId
              AND status IN (@PendingStatus, @InProgressStatus)
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                MessageId = messageId,
                Status = (short)OutboxMessageState.Published,
                ProcessedAt = DateTime.UtcNow,
                PendingStatus = (short)OutboxMessageState.Pending,
                InProgressStatus = (short)OutboxMessageState.InProgress,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task MarkFailedAsync(string messageId, string error, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(error);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        const string selectSql = """
            SELECT envelope_json::text AS EnvelopeJson
            FROM eventmesh_outbox
            WHERE message_id = @MessageId
            FOR UPDATE
            """;

        var envelopeJson = await connection.QuerySingleOrDefaultAsync<string?>(new CommandDefinition(
            selectSql,
            new { MessageId = messageId },
            transaction,
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        if (envelopeJson is null)
        {
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        var envelope = MessageEnvelopeJson.Deserialize(envelopeJson);
        var headers = new Dictionary<string, string>(envelope.Headers, StringComparer.OrdinalIgnoreCase)
        {
            ["eventmesh.lasterror"] = error,
        };

        var updatedEnvelope = envelope.WithHeaders(headers);

        const string updateSql = """
            UPDATE eventmesh_outbox
            SET status = @Status,
                envelope_json = @EnvelopeJson::jsonb
            WHERE message_id = @MessageId
            """;

        await connection.ExecuteAsync(new CommandDefinition(
            updateSql,
            new
            {
                MessageId = messageId,
                Status = (short)OutboxMessageState.Failed,
                EnvelopeJson = MessageEnvelopeJson.Serialize(updatedEnvelope),
            },
            transaction,
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<bool> TryClaimAsync(string messageId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            UPDATE eventmesh_outbox
            SET status = @InProgressStatus
            WHERE message_id = @MessageId
              AND status = @PendingStatus
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var affected = await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                MessageId = messageId,
                PendingStatus = (short)OutboxMessageState.Pending,
                InProgressStatus = (short)OutboxMessageState.InProgress,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        return affected > 0;
    }

    private static OutboxMessage MapRow(OutboxRow row)
    {
        var envelope = MessageEnvelopeJson.Deserialize(row.EnvelopeJson);
        var state = (OutboxMessageState)row.Status;
        string? lastError = null;

        if (envelope.Headers.TryGetValue("eventmesh.lasterror", out var error))
        {
            lastError = error;
        }

        return new OutboxMessage
        {
            Id = row.Id,
            Envelope = envelope,
            Destination = row.Destination,
            CreatedAt = new DateTimeOffset(DateTime.SpecifyKind(row.CreatedAt, DateTimeKind.Utc)),
            PublishedAt = row.ProcessedAt is null
                ? null
                : new DateTimeOffset(DateTime.SpecifyKind(row.ProcessedAt.Value, DateTimeKind.Utc)),
            State = state,
            LastError = lastError,
            IdempotencyKey = row.MessageId,
        };
    }

    private sealed class OutboxRow
    {
        public required string Id { get; init; }

        public required string MessageId { get; init; }

        public required string EnvelopeJson { get; init; }

        public required string Destination { get; init; }

        public DateTime CreatedAt { get; init; }

        public DateTime? ProcessedAt { get; init; }

        public short Status { get; init; }
    }
}
