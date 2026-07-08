using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Manages EventMesh plugins.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class PluginsController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="PluginsController"/> class.
    /// </summary>
    public PluginsController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Lists discovered and configured plugins.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(IReadOnlyList<PluginInfo>), StatusCodes.Status200OK)]
    public ActionResult<IReadOnlyList<PluginInfo>> GetPlugins() =>
        Ok(_observationService.GetPlugins());
}
