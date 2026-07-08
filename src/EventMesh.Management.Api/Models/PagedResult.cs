namespace EventMesh.Management.Api.Models;

/// <summary>
/// Paginated API response wrapper.
/// </summary>
public sealed class PagedResult<T>
{
    /// <summary>
    /// Gets or sets the items in the current page.
    /// </summary>
    public required IReadOnlyList<T> Items { get; init; }

    /// <summary>
    /// Gets or sets the total number of matching items.
    /// </summary>
    public int TotalCount { get; init; }

    /// <summary>
    /// Gets or sets the current page number (1-based).
    /// </summary>
    public int Page { get; init; }

    /// <summary>
    /// Gets or sets the page size.
    /// </summary>
    public int PageSize { get; init; }
}
