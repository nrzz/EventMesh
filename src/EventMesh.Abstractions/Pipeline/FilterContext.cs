namespace EventMesh.Abstractions.Pipeline;

/// <summary>
/// Base context passed through publish and consume filter pipelines.
/// </summary>
public abstract class FilterContext
{
    private readonly Dictionary<string, object?> _items = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets or sets the cancellation token for the current pipeline operation.
    /// </summary>
    public CancellationToken CancellationToken { get; set; }

    /// <summary>
    /// Gets a mutable bag of ambient values shared across filters in the pipeline.
    /// </summary>
    public IDictionary<string, object?> Items => _items;

    /// <summary>
    /// Gets or sets a value in <see cref="Items"/>.
    /// </summary>
    public void SetItem<T>(string key, T? value) where T : class => _items[key] = value;

    /// <summary>
    /// Gets a value from <see cref="Items"/>.
    /// </summary>
    public T? GetItem<T>(string key) where T : class =>
        _items.TryGetValue(key, out var value) ? value as T : null;
}
