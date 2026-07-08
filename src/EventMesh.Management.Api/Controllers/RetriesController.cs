using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Provides retry monitoring endpoints.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class RetriesController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="RetriesController"/> class.
    /// </summary>
    public RetriesController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Lists pending message retries.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(PagedResult<RetryInfo>), StatusCodes.Status200OK)]
    public ActionResult<PagedResult<RetryInfo>> GetRetries(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50) =>
        Ok(_observationService.GetRetries(page, pageSize));
}
