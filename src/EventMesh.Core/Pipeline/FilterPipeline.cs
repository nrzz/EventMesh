using EventMesh.Abstractions.Pipeline;
using Microsoft.Extensions.DependencyInjection;

namespace EventMesh.Core.Pipeline;

/// <summary>
/// Composable publish and consume filter pipeline executor.
/// </summary>
public sealed class FilterPipeline : IFilterPipeline
{
    private readonly IServiceProvider _serviceProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="FilterPipeline"/> class.
    /// </summary>
    public FilterPipeline(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    /// <inheritdoc />
    public Task PublishAsync<T>(
        PublishContext<T> context,
        FilterDelegate<PublishContext<T>> terminal,
        CancellationToken cancellationToken = default) where T : notnull
    {
        var filters = _serviceProvider.GetServices<IPublishFilter<T>>().ToArray();
        var chain = BuildChain(filters, terminal);
        return chain(context, cancellationToken);
    }

    /// <inheritdoc />
    public Task ConsumeAsync<T>(
        ConsumeContext<T> context,
        FilterDelegate<ConsumeContext<T>> terminal,
        CancellationToken cancellationToken = default) where T : notnull
    {
        var filters = _serviceProvider.GetServices<IConsumeFilter<T>>().ToArray();
        var chain = BuildChain(filters, terminal);
        return chain(context, cancellationToken);
    }

    private static FilterDelegate<TContext> BuildChain<TContext>(
        IReadOnlyList<IMessageFilter<TContext>> filters,
        FilterDelegate<TContext> terminal)
        where TContext : FilterContext
    {
        FilterDelegate<TContext> next = terminal;

        for (var index = filters.Count - 1; index >= 0; index--)
        {
            var filter = filters[index];
            var current = next;
            next = (context, cancellationToken) => filter.FilterAsync(context, current, cancellationToken);
        }

        return next;
    }
}
