using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Provides dashboard overview and connection endpoints.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class ConnectionsController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConnectionsController"/> class.
    /// </summary>
    public ConnectionsController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Gets the operational overview summary.
    /// </summary>
    [HttpGet("overview")]
    [ProducesResponseType(typeof(OverviewInfo), StatusCodes.Status200OK)]
    public ActionResult<OverviewInfo> GetOverview() => Ok(_observationService.GetOverview());

    /// <summary>
    /// Gets all monitored connections.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(IReadOnlyList<Models.ConnectionInfo>), StatusCodes.Status200OK)]
    public ActionResult<IReadOnlyList<Models.ConnectionInfo>> GetConnections() =>
        Ok(_observationService.GetConnections());
}
