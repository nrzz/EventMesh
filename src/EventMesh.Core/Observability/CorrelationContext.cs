using EventMesh.Abstractions.Observability;

namespace EventMesh.Core.Observability;

/// <summary>
/// Async-local correlation and causation context for distributed tracing.
/// </summary>
public sealed class CorrelationContext : ICorrelationContext
{
    private static readonly AsyncLocal<CorrelationScope?> CurrentScope = new();

    /// <inheritdoc />
    public string? CorrelationId => CurrentScope.Value?.CorrelationId;

    /// <inheritdoc />
    public string? CausationId => CurrentScope.Value?.CausationId;

    /// <inheritdoc />
    public IDisposable BeginScope(string? correlationId = null, string? causationId = null)
    {
        var previous = CurrentScope.Value;
        var scope = new CorrelationScope(
            correlationId ?? previous?.CorrelationId,
            causationId ?? previous?.CausationId,
            previous);

        CurrentScope.Value = scope;
        return scope;
    }

    /// <inheritdoc />
    public IDisposable BeginNewCorrelation(string? causationId = null)
    {
        return BeginScope(Guid.NewGuid().ToString("N"), causationId);
    }

    private sealed class CorrelationScope : IDisposable
    {
        private readonly CorrelationScope? _previous;
        private bool _disposed;

        public CorrelationScope(string? correlationId, string? causationId, CorrelationScope? previous)
        {
            CorrelationId = correlationId;
            CausationId = causationId;
            _previous = previous;
        }

        public string? CorrelationId { get; }

        public string? CausationId { get; }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            CurrentScope.Value = _previous;
            _disposed = true;
        }
    }
}
