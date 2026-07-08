using System.Collections.Concurrent;
using EventMesh.Abstractions.Serialization;

namespace EventMesh.Core.RequestResponse;

/// <summary>
/// Correlation-based request/response coordination with timeout support.
/// </summary>
public sealed class RequestResponseManager
{
    private readonly ConcurrentDictionary<string, PendingRequest> _pending = new(StringComparer.OrdinalIgnoreCase);
    private readonly IMessageSerializer _serializer;

    public RequestResponseManager(IMessageSerializer serializer)
    {
        _serializer = serializer;
    }

    /// <summary>
    /// Registers a pending request and returns the correlation identifier.
    /// </summary>
    public string Register<TResponse>(TimeSpan timeout, CancellationToken cancellationToken)
        where TResponse : notnull
    {
        var correlationId = Guid.NewGuid().ToString("N");
        Register<TResponse>(correlationId, timeout, cancellationToken);
        return correlationId;
    }

    /// <summary>
    /// Registers a pending request with an explicit correlation identifier.
    /// </summary>
    public void Register<TResponse>(string correlationId, TimeSpan timeout, CancellationToken cancellationToken)
        where TResponse : notnull
    {
        var registration = new PendingRequest<TResponse>();
        if (!_pending.TryAdd(correlationId, registration))
        {
            throw new InvalidOperationException($"A pending request already exists for correlation id '{correlationId}'.");
        }

        registration.RegisterCancellation(cancellationToken, correlationId, _pending);
        registration.RegisterTimeout(timeout, correlationId, _pending);
    }

    /// <summary>
    /// Waits for the correlated response.
    /// </summary>
    public Task<TResponse> WaitAsync<TResponse>(string correlationId, CancellationToken cancellationToken = default)
        where TResponse : notnull
    {
        if (!_pending.TryGetValue(correlationId, out var registration))
        {
            throw new InvalidOperationException($"No pending request found for correlation id '{correlationId}'.");
        }

        if (registration is not PendingRequest<TResponse> typed)
        {
            throw new InvalidOperationException(
                $"Pending request for correlation id '{correlationId}' does not match response type '{typeof(TResponse).FullName}'.");
        }

        return typed.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Attempts to complete a pending request from a correlated response payload.
    /// </summary>
    public async Task<bool> TryCompleteAsync(
        string correlationId,
        ReadOnlyMemory<byte> data,
        string? contentType,
        CancellationToken cancellationToken = default)
    {
        if (!_pending.TryRemove(correlationId, out var registration))
        {
            return false;
        }

        try
        {
            await registration.CompleteAsync(_serializer, data, contentType, cancellationToken);
            return true;
        }
        catch (Exception ex)
        {
            registration.TrySetException(ex);
            return true;
        }
    }

    private abstract class PendingRequest
    {
        public abstract Task CompleteAsync(
            IMessageSerializer serializer,
            ReadOnlyMemory<byte> data,
            string? contentType,
            CancellationToken cancellationToken);

        public abstract void TrySetException(Exception exception);

        public void RegisterCancellation(
            CancellationToken cancellationToken,
            string correlationId,
            ConcurrentDictionary<string, PendingRequest> pending)
        {
            if (!cancellationToken.CanBeCanceled)
            {
                return;
            }

            cancellationToken.Register(() =>
            {
                if (pending.TryRemove(correlationId, out var registration))
                {
                    registration.TrySetException(new OperationCanceledException(cancellationToken));
                }
            });
        }

        public void RegisterTimeout(
            TimeSpan timeout,
            string correlationId,
            ConcurrentDictionary<string, PendingRequest> pending)
        {
            if (timeout <= TimeSpan.Zero)
            {
                return;
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(timeout);
                    if (pending.TryRemove(correlationId, out var registration))
                    {
                        registration.TrySetException(
                            new TimeoutException(
                                $"Request timed out after {timeout} waiting for correlation id '{correlationId}'."));
                    }
                }
                catch
                {
                }
            });
        }
    }

    private sealed class PendingRequest<TResponse> : PendingRequest where TResponse : notnull
    {
        private readonly TaskCompletionSource<TResponse> _completionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<TResponse> WaitAsync(CancellationToken cancellationToken) =>
            _completionSource.Task.WaitAsync(cancellationToken);

        public override async Task CompleteAsync(
            IMessageSerializer serializer,
            ReadOnlyMemory<byte> data,
            string? contentType,
            CancellationToken cancellationToken)
        {
            var response = await serializer.DeserializeAsync<TResponse>(data, contentType, cancellationToken);
            _completionSource.TrySetResult(response);
        }

        public override void TrySetException(Exception exception) => _completionSource.TrySetException(exception);
    }
}
