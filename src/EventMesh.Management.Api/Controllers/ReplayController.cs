using EventMesh.Management.Api.Models;
using EventMesh.Management.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace EventMesh.Management.Api.Controllers;

/// <summary>
/// Manages message replay jobs.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public sealed class ReplayController : ControllerBase
{
    private readonly IMeshObservationService _observationService;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReplayController"/> class.
    /// </summary>
    public ReplayController(IMeshObservationService observationService)
    {
        _observationService = observationService;
    }

    /// <summary>
    /// Lists replay jobs.
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(IReadOnlyList<ReplayJobInfo>), StatusCodes.Status200OK)]
    public ActionResult<IReadOnlyList<ReplayJobInfo>> GetReplayJobs() =>
        Ok(_observationService.GetReplayJobs());

    /// <summary>
    /// Gets a replay job by identifier.
    /// </summary>
    [HttpGet("{id}")]
    [ProducesResponseType(typeof(ReplayJobInfo), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<ReplayJobInfo> GetReplayJob(string id)
    {
        var job = _observationService.GetReplayJob(id);
        return job is null ? NotFound() : Ok(job);
    }

    /// <summary>
    /// Starts a new replay job.
    /// </summary>
    [HttpPost]
    [ProducesResponseType(typeof(ReplayJobInfo), StatusCodes.Status202Accepted)]
    public ActionResult<ReplayJobInfo> StartReplay([FromBody] ReplayRequest request)
    {
        var job = _observationService.StartReplay(request);
        return AcceptedAtAction(nameof(GetReplayJob), new { id = job.Id }, job);
    }
}
