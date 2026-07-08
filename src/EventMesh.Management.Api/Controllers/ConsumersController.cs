using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Provides consumer monitoring endpoints.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class ConsumersController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="ConsumersController"/> class.
    /// </summary>
    public ConsumersController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Gets all active consumers.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(IReadOnlyList<ConsumerInfo>), StatusCodes.Status200OK)]
    public ActionResult<IReadOnlyList<ConsumerInfo>> GetConsumers() =>
        Ok(_observationService.GetConsumers());
}
