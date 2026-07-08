using EventMesh.Abstractions.Configuration;
using EventMesh.Plugin.Sdk;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace EventMesh.Core.Configuration;

/// <summary>
/// Default <see cref="IPluginBuilder"/> implementation backed by the host service collection.
/// </summary>
internal sealed class PluginBuilder : IPluginBuilder
{
    private readonly IServiceCollection _services;
    private readonly IFilterRegistration _filters;

    public PluginBuilder(IServiceCollection services, IFilterRegistration filters, EventMeshOptions options)
    {
        _services = services;
        _filters = filters;
        Options = options;
    }

    /// <inheritdoc />
    public EventMeshOptions Options { get; }

    /// <inheritdoc />
    public IPluginBuilder AddSingleton<TService>(TService instance) where TService : class
    {
        ArgumentNullException.ThrowIfNull(instance);
        _services.TryAddSingleton(instance);
        return this;
    }

    /// <inheritdoc />
    public IPluginBuilder AddPublishFilter<TFilter>() where TFilter : class
    {
        _filters.AddPublishFilter<TFilter>();
        return this;
    }

    /// <inheritdoc />
    public IPluginBuilder AddPublishFilter(Type openGenericFilterType)
    {
        _filters.AddPublishFilter(openGenericFilterType);
        return this;
    }

    /// <inheritdoc />
    public IPluginBuilder AddConsumeFilter<TFilter>() where TFilter : class
    {
        _filters.AddConsumeFilter<TFilter>();
        return this;
    }

    /// <inheritdoc />
    public IPluginBuilder AddConsumeFilter(Type openGenericFilterType)
    {
        _filters.AddConsumeFilter(openGenericFilterType);
        return this;
    }
}
