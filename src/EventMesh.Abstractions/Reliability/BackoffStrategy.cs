namespace EventMesh.Abstractions.Reliability;

/// <summary>
/// Strategies for calculating delay between retry attempts.
/// </summary>
public enum BackoffStrategy
{
    /// <summary>
    /// Uses a fixed delay between retries.
    /// </summary>
    Fixed = 0,

    /// <summary>
    /// Increases delay linearly with each retry attempt.
    /// </summary>
    Linear = 1,

    /// <summary>
    /// Increases delay exponentially with each retry attempt.
    /// </summary>
    Exponential = 2,

    /// <summary>
    /// Uses exponential backoff with randomized jitter.
    /// </summary>
    ExponentialWithJitter = 3,
}
