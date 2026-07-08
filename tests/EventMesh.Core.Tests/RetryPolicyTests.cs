using EventMesh.Abstractions.Reliability;
using FluentAssertions;

namespace EventMesh.Core.Tests;

public sealed class RetryPolicyTests
{
    [Fact]
    public void CalculateDelay_Fixed_ReturnsInitialDelay()
    {
        var policy = new RetryPolicy
        {
            BackoffStrategy = BackoffStrategy.Fixed,
            InitialDelay = TimeSpan.FromSeconds(2),
        };

        policy.CalculateDelay(0).Should().Be(TimeSpan.FromSeconds(2));
        policy.CalculateDelay(3).Should().Be(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public void CalculateDelay_Linear_ScalesWithAttempt()
    {
        var policy = new RetryPolicy
        {
            BackoffStrategy = BackoffStrategy.Linear,
            InitialDelay = TimeSpan.FromSeconds(1),
        };

        policy.CalculateDelay(0).Should().Be(TimeSpan.FromSeconds(1));
        policy.CalculateDelay(1).Should().Be(TimeSpan.FromSeconds(2));
        policy.CalculateDelay(2).Should().Be(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public void CalculateDelay_Exponential_AppliesMultiplier()
    {
        var policy = new RetryPolicy
        {
            BackoffStrategy = BackoffStrategy.Exponential,
            InitialDelay = TimeSpan.FromSeconds(1),
            BackoffMultiplier = 2,
        };

        policy.CalculateDelay(0).Should().Be(TimeSpan.FromSeconds(1));
        policy.CalculateDelay(1).Should().Be(TimeSpan.FromSeconds(2));
        policy.CalculateDelay(2).Should().Be(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public void CalculateDelay_ExponentialWithJitter_StaysWithinJitterBounds()
    {
        var policy = new RetryPolicy
        {
            BackoffStrategy = BackoffStrategy.ExponentialWithJitter,
            InitialDelay = TimeSpan.FromSeconds(1),
            BackoffMultiplier = 2,
        };

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var baseDelay = TimeSpan.FromSeconds(Math.Pow(2, attempt));
            var delay = policy.CalculateDelay(attempt);

            delay.Should().BeGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * 0.8));
            delay.Should().BeLessThanOrEqualTo(TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * 1.2));
        }
    }

    [Fact]
    public void CalculateDelay_CapsAtMaxDelay()
    {
        var policy = new RetryPolicy
        {
            BackoffStrategy = BackoffStrategy.Exponential,
            InitialDelay = TimeSpan.FromSeconds(10),
            BackoffMultiplier = 2,
            MaxDelay = TimeSpan.FromSeconds(30),
        };

        policy.CalculateDelay(10).Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void CalculateDelay_NegativeAttempt_Throws()
    {
        var policy = new RetryPolicy();

        var action = () => policy.CalculateDelay(-1);

        action.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void IsRetryable_WhenNoExceptionsConfigured_ReturnsTrue()
    {
        var policy = new RetryPolicy();

        policy.IsRetryable(new InvalidOperationException()).Should().BeTrue();
    }

    [Fact]
    public void IsRetryable_WhenExceptionTypeMatches_ReturnsTrue()
    {
        var policy = new RetryPolicy
        {
            RetryableExceptions = new HashSet<Type> { typeof(IOException) },
        };

        policy.IsRetryable(new IOException()).Should().BeTrue();
        policy.IsRetryable(new InvalidOperationException()).Should().BeFalse();
    }

    [Fact]
    public void IsRetryable_WhenDerivedExceptionMatches_ReturnsTrue()
    {
        var policy = new RetryPolicy
        {
            RetryableExceptions = new HashSet<Type> { typeof(Exception) },
        };

        policy.IsRetryable(new InvalidOperationException()).Should().BeTrue();
    }

    [Fact]
    public void IsRetryable_NullException_Throws()
    {
        var policy = new RetryPolicy();

        var action = () => policy.IsRetryable(null!);

        action.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Clone_CreatesIndependentCopy()
    {
        var policy = new RetryPolicy
        {
            MaxRetries = 5,
            BackoffStrategy = BackoffStrategy.Linear,
            InitialDelay = TimeSpan.FromMilliseconds(500),
            MaxDelay = TimeSpan.FromMinutes(1),
            BackoffMultiplier = 3,
            RetryableExceptions = new HashSet<Type> { typeof(TimeoutException) },
        };

        var clone = policy.Clone();

        clone.Should().NotBeSameAs(policy);
        clone.MaxRetries.Should().Be(policy.MaxRetries);
        clone.BackoffStrategy.Should().Be(policy.BackoffStrategy);
        clone.InitialDelay.Should().Be(policy.InitialDelay);
        clone.MaxDelay.Should().Be(policy.MaxDelay);
        clone.BackoffMultiplier.Should().Be(policy.BackoffMultiplier);
        clone.RetryableExceptions.Should().BeEquivalentTo(policy.RetryableExceptions);
        clone.RetryableExceptions.Should().NotBeSameAs(policy.RetryableExceptions);
    }
}
