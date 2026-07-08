namespace EventMesh.Abstractions.Pipeline;

/// <summary>
/// Executes composable publish and consume filter pipelines.
/// </summary>
public interface IFilterPipeline
{
    /// <summary>
    /// Executes the publish filter pipeline and invokes the terminal handler.
    /// </summary>
    /// <typeparam name="T">The message payload type.</typeparam>
    /// <param name="context">The publish context.</param>
    /// <param name="terminal">The terminal handler invoked after all filters.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task PublishAsync<T>(
        PublishContext<T> context,
        FilterDelegate<PublishContext<T>> terminal,
        CancellationToken cancellationToken = default) where T : notnull;

    /// <summary>
    /// Executes the consume filter pipeline and invokes the terminal handler.
    /// </summary>
    /// <typeparam name="T">The message payload type.</typeparam>
    /// <param name="context">The consume context.</param>
    /// <param name="terminal">The terminal handler invoked after all filters.</param>
    /// <param name="cancellationToken">A token used to cancel the operation.</param>
    Task ConsumeAsync<T>(
        ConsumeContext<T> context,
        FilterDelegate<ConsumeContext<T>> terminal,
        CancellationToken cancellationToken = default) where T : notnull;
}
