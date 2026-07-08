using EventMesh.Abstractions.Configuration;

namespace EventMesh.Plugin.Sdk;

/// <summary>
/// Fluent builder surface used by plugins to register services and pipeline filters.
/// </summary>
public interface IPluginBuilder
{
    /// <summary>
    /// Gets the EventMesh options being configured.
    /// </summary>
    EventMeshOptions Options { get; }

    /// <summary>
    /// Registers a singleton service instance.
    /// </summary>
    IPluginBuilder AddSingleton<TService>(TService instance) where TService : class;

    /// <summary>
    /// Registers a publish filter for all message types.
    /// </summary>
    IPluginBuilder AddPublishFilter<TFilter>() where TFilter : class;

    /// <summary>
    /// Registers an open-generic publish filter for all message types.
    /// </summary>
    IPluginBuilder AddPublishFilter(Type openGenericFilterType);

    /// <summary>
    /// Registers a consume filter for all message types.
    /// </summary>
    IPluginBuilder AddConsumeFilter<TFilter>() where TFilter : class;

    /// <summary>
    /// Registers an open-generic consume filter for all message types.
    /// </summary>
    IPluginBuilder AddConsumeFilter(Type openGenericFilterType);
}
