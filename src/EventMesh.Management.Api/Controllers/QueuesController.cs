using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Manages messaging queues.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class QueuesController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="QueuesController"/> class.
    /// </summary>
    public QueuesController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Lists queues with optional search and pagination.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(PagedResult<QueueInfo>), StatusCodes.Status200OK)]
    public ActionResult<PagedResult<QueueInfo>> GetQueues(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        [FromQuery] string? search = null) =>
        Ok(_observationService.GetQueues(page, pageSize, search));

    /// <summary>
    /// Gets a queue by name.
    /// </summary>
    [HttpGet("{name}")]
    [ProducesResponseType(typeof(QueueInfo), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<QueueInfo> GetQueue(string name)
    {
        var queue = _observationService.GetQueue(name);
        return queue is null ? NotFound() : Ok(queue);
    }

    /// <summary>
    /// Creates or updates a queue.
    /// </summary>
    [HttpPost]
    [ProducesResponseType(typeof(QueueInfo), StatusCodes.Status201Created)]
    public ActionResult<QueueInfo> CreateQueue([FromBody] CreateQueueRequest request)
    {
        var queue = _observationService.CreateQueue(request);
        return CreatedAtAction(nameof(GetQueue), new { name = queue.Name }, queue);
    }

    /// <summary>
    /// Purges all messages from a queue.
    /// </summary>
    [HttpPost("{name}/purge")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public IActionResult PurgeQueue(string name) =>
        _observationService.PurgeQueue(name) ? NoContent() : NotFound();
}
