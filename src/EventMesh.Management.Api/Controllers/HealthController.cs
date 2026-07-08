using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Provides cluster health endpoints for the management dashboard.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class HealthController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="HealthController"/> class.
    /// </summary>
    public HealthController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Gets aggregated cluster health information.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(ClusterHealthInfo), StatusCodes.Status200OK)]
    public ActionResult<ClusterHealthInfo> GetClusterHealth() =>
        Ok(_observationService.GetClusterHealth());

    /// <summary>
    /// Gets a lightweight liveness probe response.
    /// </summary>
    [HttpGet("live")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public IActionResult GetLiveness() => Ok(new { status = "alive", timestamp = DateTimeOffset.UtcNow });

    /// <summary>
    /// Gets a readiness probe based on connection health.
    /// </summary>
    [HttpGet("ready")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status503ServiceUnavailable)]
    public IActionResult GetReadiness()
    {
        var health = _observationService.GetClusterHealth();
        return health.Status.Equals("healthy", StringComparison.OrdinalIgnoreCase)
            ? Ok(health)
            : StatusCode(StatusCodes.Status503ServiceUnavailable, health);
    }
}
