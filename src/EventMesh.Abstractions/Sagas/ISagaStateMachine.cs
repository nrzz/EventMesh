namespace EventMesh.Abstractions.Sagas;

/// <summary>
/// Represents a long-running process coordinated through messages and explicit state transitions.
/// </summary>
/// <typeparam name="TState">The saga state enum type.</typeparam>
public interface ISagaStateMachine<TState> where TState : struct, Enum
{
    /// <summary>
    /// Gets the unique saga instance identifier.
    /// </summary>
    string SagaId { get; }

    /// <summary>
    /// Gets the current saga state.
    /// </summary>
    TState CurrentState { get; }

    /// <summary>
    /// Gets the correlation identifier associated with the saga instance.
    /// </summary>
    string CorrelationId { get; }

    /// <summary>
    /// Handles an incoming message and applies any resulting state transitions.
    /// </summary>
    /// <param name="message">The message to handle.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task HandleAsync(object message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Transitions the saga to the specified state.
    /// </summary>
    /// <param name="newState">The target state.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task TransitionToAsync(TState newState, CancellationToken cancellationToken = default);
}
