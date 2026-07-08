using EventMesh.Management.Api.Models;
using Microsoft.AspNetCore.SignalR;

namespace EventMesh.Management.Api.Hubs;

/// <summary>
/// SignalR hub for real-time management dashboard updates.
/// </summary>
public sealed class EventMeshHub : Hub
{
    /// <summary>
    /// Subscribes the connection to overview updates.
    /// </summary>
    public Task SubscribeOverview() => Groups.AddToGroupAsync(Context.ConnectionId, "overview");

    /// <summary>
    /// Subscribes the connection to metrics updates.
    /// </summary>
    public Task SubscribeMetrics() => Groups.AddToGroupAsync(Context.ConnectionId, "metrics");

    /// <summary>
    /// Subscribes the connection to connection health updates.
    /// </summary>
    public Task SubscribeConnections() => Groups.AddToGroupAsync(Context.ConnectionId, "connections");

    /// <summary>
    /// Broadcasts a custom notification to all connected dashboard clients.
    /// </summary>
    public Task BroadcastNotification(string message) =>
        Clients.All.SendAsync("NotificationReceived", new { message, timestamp = DateTimeOffset.UtcNow });
}
