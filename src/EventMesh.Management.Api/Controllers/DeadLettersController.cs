using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Provides dead-letter queue management endpoints.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class DeadLettersController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="DeadLettersController"/> class.
    /// </summary>
    public DeadLettersController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Lists dead-lettered messages.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(PagedResult<DeadLetterInfo>), StatusCodes.Status200OK)]
    public ActionResult<PagedResult<DeadLetterInfo>> GetDeadLetters(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50) =>
        Ok(_observationService.GetDeadLetters(page, pageSize));

    /// <summary>
    /// Reprocesses a dead-lettered message.
    /// </summary>
    [HttpPost("{id}/reprocess")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public IActionResult Reprocess(string id, [FromBody] ReprocessDeadLetterRequest request)
    {
        request ??= new ReprocessDeadLetterRequest();
        return _observationService.ReprocessDeadLetter(id, request) ? NoContent() : NotFound();
    }
}
