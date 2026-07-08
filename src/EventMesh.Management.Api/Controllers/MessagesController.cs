using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Provides message inspection endpoints.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class MessagesController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="MessagesController"/> class.
    /// </summary>
    public MessagesController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Lists messages with optional filters and pagination.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(PagedResult<MessageInfo>), StatusCodes.Status200OK)]
    public ActionResult<PagedResult<MessageInfo>> GetMessages(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50,
        [FromQuery] string? source = null,
        [FromQuery] string? type = null) =>
        Ok(_observationService.GetMessages(page, pageSize, source, type));

    /// <summary>
    /// Gets a message by identifier.
    /// </summary>
    [HttpGet("{id}")]
    [ProducesResponseType(typeof(MessageInfo), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<MessageInfo> GetMessage(string id)
    {
        var message = _observationService.GetMessage(id);
        return message is null ? NotFound() : Ok(message);
    }
}
