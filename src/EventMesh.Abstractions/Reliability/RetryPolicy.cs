namespace EventMesh.Abstractions.Reliability;

/// <summary>
/// Declarative retry policy applied when message handling or transport operations fail.
/// </summary>
public sealed class RetryPolicy
{
    /// <summary>
    /// Gets or sets the maximum number of retry attempts after the initial try.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Gets or sets the backoff strategy used to calculate retry delays.
    /// </summary>
    public BackoffStrategy BackoffStrategy { get; set; } = BackoffStrategy.ExponentialWithJitter;

    /// <summary>
    /// Gets or sets the initial delay before the first retry.
    /// </summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the maximum delay between retry attempts.
    /// </summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets the multiplier applied for exponential backoff calculations.
    /// </summary>
    public double BackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Gets or sets the exception types that should trigger a retry.
    /// When empty, all exceptions are considered retryable.
    /// </summary>
    public ISet<Type> RetryableExceptions { get; set; } = new HashSet<Type>();

    /// <summary>
    /// Calculates the delay before the specified retry attempt.
    /// </summary>
    /// <param name="attempt">The zero-based retry attempt number.</param>
    /// <returns>The delay to wait before the retry.</returns>
    public TimeSpan CalculateDelay(int attempt)
    {
        if (attempt < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(attempt), attempt, "Attempt must be non-negative.");
        }

        var delay = BackoffStrategy switch
        {
            BackoffStrategy.Fixed => InitialDelay,
            BackoffStrategy.Linear => TimeSpan.FromMilliseconds(InitialDelay.TotalMilliseconds * (attempt + 1)),
            BackoffStrategy.Exponential => TimeSpan.FromMilliseconds(
                InitialDelay.TotalMilliseconds * Math.Pow(BackoffMultiplier, attempt)),
            BackoffStrategy.ExponentialWithJitter => ApplyJitter(
                TimeSpan.FromMilliseconds(InitialDelay.TotalMilliseconds * Math.Pow(BackoffMultiplier, attempt))),
            _ => InitialDelay,
        };

        return delay <= MaxDelay ? delay : MaxDelay;
    }

    /// <summary>
    /// Determines whether the specified exception should be retried.
    /// </summary>
    public bool IsRetryable(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (RetryableExceptions.Count == 0)
        {
            return true;
        }

        var exceptionType = exception.GetType();
        return RetryableExceptions.Any(retryable => retryable.IsAssignableFrom(exceptionType));
    }

    /// <summary>
    /// Creates a shallow copy of the current retry policy.
    /// </summary>
    public RetryPolicy Clone() => new()
    {
        MaxRetries = MaxRetries,
        BackoffStrategy = BackoffStrategy,
        InitialDelay = InitialDelay,
        MaxDelay = MaxDelay,
        BackoffMultiplier = BackoffMultiplier,
        RetryableExceptions = new HashSet<Type>(RetryableExceptions),
    };

    private static TimeSpan ApplyJitter(TimeSpan delay)
    {
        var jitterFactor = Random.Shared.NextDouble() * 0.4 + 0.8;
        return TimeSpan.FromMilliseconds(delay.TotalMilliseconds * jitterFactor);
    }
}
