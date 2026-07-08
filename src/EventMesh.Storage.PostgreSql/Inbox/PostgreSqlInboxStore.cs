using Dapper;
using EventMesh.Abstractions.Envelope;
using EventMesh.Abstractions.Reliability;
using EventMesh.Storage.PostgreSql.Schema;
using Npgsql;

namespace EventMesh.Storage.PostgreSql.Inbox;

/// <summary>
/// PostgreSQL-backed implementation of <see cref="IInboxStore"/> for idempotent consumption.
/// </summary>
public sealed class PostgreSqlInboxStore : IInboxStore
{
    private const string DefaultConsumerGroup = "default";
    private const string InboxSource = "eventmesh/inbox";
    private const string InboxRecordType = "eventmesh.inbox.record";

    private readonly NpgsqlDataSource _dataSource;
    private readonly SchemaInitializer _schemaInitializer;

    public PostgreSqlInboxStore(NpgsqlDataSource dataSource, SchemaInitializer schemaInitializer)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _schemaInitializer = schemaInitializer ?? throw new ArgumentNullException(nameof(schemaInitializer));
    }

    /// <inheritdoc />
    public async Task<bool> TryRegisterAsync(InboxMessage message, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(message.IdempotencyKey);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var consumerGroup = NormalizeConsumerGroup(message.ConsumerId);

        const string sql = """
            INSERT INTO eventmesh_inbox
                (id, message_id, consumer_group, processed_at)
            VALUES
                (@Id, @MessageId, @ConsumerGroup, NULL)
            ON CONFLICT (message_id, consumer_group) DO NOTHING
            RETURNING id
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var insertedId = await connection.QuerySingleOrDefaultAsync<string?>(new CommandDefinition(
            sql,
            new
            {
                message.Id,
                MessageId = message.IdempotencyKey,
                ConsumerGroup = consumerGroup,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        return insertedId is not null;
    }

    /// <inheritdoc />
    public async Task MarkProcessedAsync(string idempotencyKey, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(idempotencyKey);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            UPDATE eventmesh_inbox
            SET processed_at = @ProcessedAt
            WHERE message_id = @MessageId
              AND processed_at IS NULL
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new
            {
                MessageId = idempotencyKey,
                ProcessedAt = DateTime.UtcNow,
            },
            cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task MarkFailedAsync(string idempotencyKey, string error, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(idempotencyKey);
        ArgumentException.ThrowIfNullOrWhiteSpace(error);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            DELETE FROM eventmesh_inbox
            WHERE message_id = @MessageId
              AND processed_at IS NULL
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await connection.ExecuteAsync(new CommandDefinition(
            sql,
            new { MessageId = idempotencyKey },
            cancellationToken: cancellationToken)).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<InboxMessage?> GetByIdempotencyKeyAsync(
        string idempotencyKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(idempotencyKey);

        await _schemaInitializer.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        const string sql = """
            SELECT
                id AS Id,
                message_id AS MessageId,
                consumer_group AS ConsumerGroup,
                processed_at AS ProcessedAt
            FROM eventmesh_inbox
            WHERE message_id = @MessageId
            ORDER BY processed_at DESC NULLS LAST, id
            LIMIT 1
            """;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var row = await connection.QuerySingleOrDefaultAsync<InboxRow>(new CommandDefinition(
            sql,
            new { MessageId = idempotencyKey },
            cancellationToken: cancellationToken)).ConfigureAwait(false);

        return row is null ? null : MapRow(row);
    }

    private static InboxMessage MapRow(InboxRow row)
    {
        var processedAt = row.ProcessedAt is null
            ? (DateTimeOffset?)null
            : new DateTimeOffset(DateTime.SpecifyKind(row.ProcessedAt.Value, DateTimeKind.Utc));

        return new InboxMessage
        {
            Id = row.Id,
            IdempotencyKey = row.MessageId,
            ConsumerId = row.ConsumerGroup,
            Envelope = CreatePlaceholderEnvelope(row.MessageId),
            ReceivedAt = processedAt ?? DateTimeOffset.UtcNow,
            ProcessedAt = processedAt,
            State = processedAt is null ? InboxMessageState.Received : InboxMessageState.Processed,
        };
    }

    private static MessageEnvelope CreatePlaceholderEnvelope(string messageId)
    {
        return MessageEnvelope.Create()
            .WithId(messageId)
            .WithSource(InboxSource)
            .WithType(InboxRecordType)
            .Build();
    }

    private static string NormalizeConsumerGroup(string? consumerId)
    {
        return string.IsNullOrWhiteSpace(consumerId) ? DefaultConsumerGroup : consumerId;
    }

    private sealed class InboxRow
    {
        public required string Id { get; init; }

        public required string MessageId { get; init; }

        public required string ConsumerGroup { get; init; }

        public DateTime? ProcessedAt { get; init; }
    }
}
