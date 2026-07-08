namespace EventMesh.Abstractions.Observability;

/// <summary>
/// Provides ambient correlation and causation identifiers for distributed tracing.
/// </summary>
public interface ICorrelationContext
{
    /// <summary>
    /// Gets the current correlation identifier.
    /// </summary>
    string? CorrelationId { get; }

    /// <summary>
    /// Gets the current causation identifier.
    /// </summary>
    string? CausationId { get; }

    /// <summary>
    /// Begins a new correlation scope with the specified identifiers.
    /// </summary>
    /// <param name="correlationId">The correlation identifier.</param>
    /// <param name="causationId">The optional causation identifier.</param>
    /// <returns>A disposable scope that restores the previous context when disposed.</returns>
    IDisposable BeginScope(string? correlationId = null, string? causationId = null);

    /// <summary>
    /// Generates a new correlation identifier and begins a scope with it.
    /// </summary>
    /// <param name="causationId">The optional causation identifier.</param>
    /// <returns>A disposable scope that restores the previous context when disposed.</returns>
    IDisposable BeginNewCorrelation(string? causationId = null);
}
