namespace EventMesh.Abstractions.Pipeline;

/// <summary>
/// Delegate representing the next filter or terminal handler in a pipeline.
/// </summary>
/// <typeparam name="TContext">The filter context type.</typeparam>
/// <param name="context">The current filter context.</param>
/// <param name="cancellationToken">A token used to cancel the operation.</param>
public delegate Task FilterDelegate<TContext>(TContext context, CancellationToken cancellationToken = default)
    where TContext : FilterContext;

/// <summary>
/// A filter that participates in publish or consume pipelines.
/// </summary>
/// <typeparam name="TContext">The filter context type.</typeparam>
public interface IMessageFilter<TContext> where TContext : FilterContext
{
    /// <summary>
    /// Executes the filter and invokes the next delegate in the pipeline.
    /// </summary>
    /// <param name="context">The current filter context.</param>
    /// <param name="next">The next filter or terminal handler.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task FilterAsync(TContext context, FilterDelegate<TContext> next, CancellationToken cancellationToken = default);
}

/// <summary>
/// A filter executed during message publish operations.
/// </summary>
/// <typeparam name="T">The message payload type.</typeparam>
public interface IPublishFilter<T> : IMessageFilter<PublishContext<T>> where T : notnull;

/// <summary>
/// A filter executed during message consume operations.
/// </summary>
/// <typeparam name="T">The message payload type.</typeparam>
public interface IConsumeFilter<T> : IMessageFilter<ConsumeContext<T>> where T : notnull;
