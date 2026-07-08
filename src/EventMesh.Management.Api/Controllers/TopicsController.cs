using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Manages messaging topics.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class TopicsController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="TopicsController"/> class.
    /// </summary>
    public TopicsController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Lists topics with optional search and pagination.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(PagedResult<TopicInfo>), StatusCodes.Status200OK)]
    public ActionResult<PagedResult<TopicInfo>> GetTopics(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        [FromQuery] string? search = null) =>
        Ok(_observationService.GetTopics(page, pageSize, search));

    /// <summary>
    /// Gets a topic by name.
    /// </summary>
    [HttpGet("{name}")]
    [ProducesResponseType(typeof(TopicInfo), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<TopicInfo> GetTopic(string name)
    {
        var topic = _observationService.GetTopic(name);
        return topic is null ? NotFound() : Ok(topic);
    }

    /// <summary>
    /// Creates or updates a topic.
    /// </summary>
    [HttpPost]
    [ProducesResponseType(typeof(TopicInfo), StatusCodes.Status201Created)]
    public ActionResult<TopicInfo> CreateTopic([FromBody] CreateTopicRequest request)
    {
        var topic = _observationService.CreateTopic(request);
        return CreatedAtAction(nameof(GetTopic), new { name = topic.Name }, topic);
    }

    /// <summary>
    /// Deletes a topic.
    /// </summary>
    [HttpDelete("{name}")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public IActionResult DeleteTopic(string name) =>
        _observationService.DeleteTopic(name) ? NoContent() : NotFound();
}
