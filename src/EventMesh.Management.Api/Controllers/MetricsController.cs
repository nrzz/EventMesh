using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Exposes EventMesh operational metrics.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class MetricsController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="MetricsController"/> class.
    /// </summary>
    public MetricsController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Gets the current metrics snapshot.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(MetricsSnapshot), StatusCodes.Status200OK)]
    public ActionResult<MetricsSnapshot> GetMetrics() => Ok(_observationService.GetMetrics());
}
