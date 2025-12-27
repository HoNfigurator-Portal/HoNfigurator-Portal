using HoNfigurator.ManagementPortal.Data;
using HoNfigurator.ManagementPortal.Models;
using HoNfigurator.ManagementPortal.Services;
using HoNfigurator.ManagementPortal.Hubs;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;
using System.Net.Http.Json;

namespace HoNfigurator.ManagementPortal.Endpoints;

public static class PortalEndpoints
{
    private static readonly HttpClient _proxyClient = new HttpClient
    {
        Timeout = TimeSpan.FromSeconds(30)
    };

    /// <summary>
    /// Helper: Check if user has access to a server with minimum required role
    /// Returns the server if access granted, null otherwise
    /// </summary>
    private static async Task<(RegisteredServer? Server, ServerRole? Role, string? Error)> GetServerWithAccess(
        PortalDbContext db,
        PortalUser user,
        string serverId,
        ServerRole minimumRole = ServerRole.Viewer)
    {
        var server = await db.Servers.FirstOrDefaultAsync(s => s.ServerId == serverId);
        if (server == null)
        {
            return (null, null, "Server not found");
        }

        // SuperAdmin has full access
        if (user.IsSuperAdmin)
        {
            return (server, ServerRole.Owner, null);
        }

        // Check if original owner
        if (server.OwnerId == user.Id)
        {
            return (server, ServerRole.Owner, null);
        }

        // Check shared access
        var access = await db.ServerAccess
            .FirstOrDefaultAsync(a => a.ServerId == server.Id 
                && (a.DiscordId == user.DiscordId || a.UserId == user.Id));

        if (access == null)
        {
            return (null, null, "Access denied");
        }

        // Check if role meets minimum requirement
        if (access.Role < minimumRole)
        {
            return (null, access.Role, $"Insufficient permissions. Required: {minimumRole}, Your role: {access.Role}");
        }

        return (server, access.Role, null);
    }

    public static void MapPortalEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/api")
            .WithTags("Portal")
            .WithOpenApi();

        // User's Servers Management (requires auth)
        group.MapGet("/my-servers", GetMyServers)
            .WithName("GetMyServers")
            .WithDescription("Get current user's registered servers");

        group.MapPost("/my-servers", AddServer)
            .WithName("AddServer")
            .WithDescription("Add a new server host");

        group.MapPut("/my-servers/{serverId}", UpdateServer)
            .WithName("UpdateServer")
            .WithDescription("Update server details");

        group.MapDelete("/my-servers/{serverId}", DeleteServer)
            .WithName("DeleteServer")
            .WithDescription("Remove a server");

        // Dashboard Data (requires auth)
        group.MapGet("/dashboard", GetDashboard)
            .WithName("GetDashboard")
            .WithDescription("Get aggregated dashboard summary for user's servers");

        // Status Reporting (from HoNfigurator instances - uses API key)
        group.MapPost("/status", ReportStatus)
            .WithName("ReportStatus")
            .WithDescription("Report server status (called by HoNfigurator instances)");

        // Remote Management (proxy to HoNfigurator)
        group.MapGet("/servers/{serverId}/details", GetServerDetails)
            .WithName("GetServerDetails")
            .WithDescription("Get detailed server information from HoNfigurator");

        group.MapPost("/servers/{serverId}/instances/{instanceId}/start", StartGameInstance)
            .WithName("StartGameInstance")
            .WithDescription("Start a game server instance");

        group.MapPost("/servers/{serverId}/instances/{instanceId}/stop", StopGameInstance)
            .WithName("StopGameInstance")
            .WithDescription("Stop a game server instance");

        group.MapPost("/servers/{serverId}/instances/{instanceId}/restart", RestartGameInstance)
            .WithName("RestartGameInstance")
            .WithDescription("Restart a game server instance");

        group.MapPost("/servers/{serverId}/start-all", StartAllInstances)
            .WithName("StartAllInstances")
            .WithDescription("Start all game server instances");

        group.MapPost("/servers/{serverId}/stop-all", StopAllInstances)
            .WithName("StopAllInstances")
            .WithDescription("Stop all game server instances");

        group.MapPost("/servers/{serverId}/restart-all", RestartAllInstances)
            .WithName("RestartAllInstances")
            .WithDescription("Restart all game server instances");

        group.MapPost("/servers/{serverId}/scale", ScaleInstances)
            .WithName("ScaleInstances")
            .WithDescription("Scale to target number of instances");

        group.MapPost("/servers/{serverId}/instances/add", AddInstance)
            .WithName("AddInstance")
            .WithDescription("Add a new game server instance");

        group.MapPost("/servers/{serverId}/instances/add-all", AddAllInstances)
            .WithName("AddAllInstances")
            .WithDescription("Add all instances up to maximum CPU capacity");

        group.MapPost("/servers/{serverId}/instances/{instanceId}/delete", DeleteInstance)
            .WithName("DeleteInstance")
            .WithDescription("Delete a game server instance");

        group.MapPost("/servers/{serverId}/broadcast", BroadcastMessage)
            .WithName("BroadcastMessage")
            .WithDescription("Broadcast message to all players");

        // Configuration Management
        group.MapGet("/servers/{serverId}/config", GetServerConfig)
            .WithName("GetServerConfig")
            .WithDescription("Get server configuration from HoNfigurator");

        group.MapPost("/servers/{serverId}/config", UpdateServerConfig)
            .WithName("UpdateServerConfig")
            .WithDescription("Update server configuration (simple)");

        group.MapPost("/servers/{serverId}/config/full", UpdateServerConfigFull)
            .WithName("UpdateServerConfigFull")
            .WithDescription("Update full server configuration");

        // Replays Management
        group.MapGet("/servers/{serverId}/replays", GetReplays)
            .WithName("GetReplays")
            .WithDescription("Get list of replays from server");

        group.MapGet("/servers/{serverId}/replays/stats", GetReplayStats)
            .WithName("GetReplayStats")
            .WithDescription("Get replay statistics from server");

        group.MapGet("/servers/{serverId}/replays/download/{filename}", DownloadReplay)
            .WithName("DownloadReplay")
            .WithDescription("Download a replay file");

        group.MapPost("/servers/{serverId}/replays/archive", ArchiveReplays)
            .WithName("ArchiveReplays")
            .WithDescription("Archive old replays");

        group.MapPost("/servers/{serverId}/replays/cleanup", CleanupReplays)
            .WithName("CleanupReplays")
            .WithDescription("Cleanup old archived replays");

        group.MapDelete("/servers/{serverId}/replays/{filename}", DeleteReplayFile)
            .WithName("DeleteReplayFile")
            .WithDescription("Delete a replay file");

        // ==================== LOGS ====================
        group.MapGet("/servers/{serverId}/logs/{instanceId}", GetServerLogs)
            .WithName("GetServerLogs")
            .WithDescription("Get logs for a server instance");

        group.MapGet("/servers/{serverId}/logs/{instanceId}/download", DownloadServerLogs)
            .WithName("DownloadServerLogs")
            .WithDescription("Download log file for a server instance");

        // ==================== STATISTICS ====================
        group.MapGet("/servers/{serverId}/statistics/summary", GetStatisticsSummary)
            .WithName("GetStatisticsSummary")
            .WithDescription("Get overall statistics summary");

        group.MapGet("/servers/{serverId}/statistics/matches", GetRecentMatches)
            .WithName("GetRecentMatches")
            .WithDescription("Get recent matches");

        group.MapGet("/servers/{serverId}/statistics/players/top", GetTopPlayers)
            .WithName("GetTopPlayers")
            .WithDescription("Get top players by win rate");

        group.MapGet("/servers/{serverId}/statistics/players/active", GetMostActivePlayers)
            .WithName("GetMostActivePlayers")
            .WithDescription("Get most active players");

        group.MapGet("/servers/{serverId}/statistics/daily", GetDailyStats)
            .WithName("GetDailyStats")
            .WithDescription("Get daily statistics");

        // ==================== HEALTH ====================
        group.MapGet("/servers/{serverId}/health/checks", GetHealthChecks)
            .WithName("GetHealthChecks")
            .WithDescription("Get health check results");

        group.MapGet("/servers/{serverId}/health/resources", GetSystemResources)
            .WithName("GetSystemResources")
            .WithDescription("Get CPU, memory, disk usage");

        group.MapPost("/servers/{serverId}/health/run", RunHealthChecks)
            .WithName("RunHealthChecks")
            .WithDescription("Manually run health checks");

        group.MapGet("/servers/{serverId}/health/enhanced", GetEnhancedHealthChecks)
            .WithName("GetEnhancedHealthChecks")
            .WithDescription("Get enhanced health checks");

        // ==================== METRICS ====================
        group.MapGet("/servers/{serverId}/metrics", GetMetrics)
            .WithName("GetMetrics")
            .WithDescription("Get current metrics");

        group.MapGet("/servers/{serverId}/metrics/history", GetMetricsHistory)
            .WithName("GetMetricsHistory")
            .WithDescription("Get metrics history");

        // ==================== PERFORMANCE ====================
        group.MapGet("/servers/{serverId}/performance/current", GetCurrentPerformance)
            .WithName("GetCurrentPerformance")
            .WithDescription("Get current performance metrics");

        group.MapGet("/servers/{serverId}/performance/history", GetPerformanceHistory)
            .WithName("GetPerformanceHistory")
            .WithDescription("Get performance history");

        group.MapGet("/servers/{serverId}/performance/summary", GetPerformanceSummary)
            .WithName("GetPerformanceSummary")
            .WithDescription("Get performance summary");

        // ==================== PATCHING ====================
        group.MapGet("/servers/{serverId}/patching/status", GetPatchStatus)
            .WithName("GetPatchStatus")
            .WithDescription("Get patch status");

        group.MapGet("/servers/{serverId}/patching/check", CheckForPatches)
            .WithName("CheckForPatches")
            .WithDescription("Check for available patches");

        group.MapPost("/servers/{serverId}/patching/apply", ApplyPatch)
            .WithName("ApplyPatch")
            .WithDescription("Apply available patch");

        // ==================== AUTOPING ====================
        group.MapGet("/servers/{serverId}/autoping/status", GetAutoPingStatus)
            .WithName("GetAutoPingStatus")
            .WithDescription("Get AutoPing status");

        group.MapPost("/servers/{serverId}/autoping/start", StartAutoPing)
            .WithName("StartAutoPing")
            .WithDescription("Start AutoPing listener");

        group.MapPost("/servers/{serverId}/autoping/stop", StopAutoPing)
            .WithName("StopAutoPing")
            .WithDescription("Stop AutoPing listener");

        // ==================== EVENTS ====================
        group.MapGet("/servers/{serverId}/events", GetEvents)
            .WithName("GetEvents")
            .WithDescription("Get recent events");

        group.MapGet("/servers/{serverId}/events/stats", GetEventStats)
            .WithName("GetEventStats")
            .WithDescription("Get event statistics");

        // ==================== MQTT ====================
        group.MapGet("/servers/{serverId}/mqtt/status", GetMqttStatus)
            .WithName("GetMqttStatus")
            .WithDescription("Get MQTT connection status");

        group.MapPost("/servers/{serverId}/mqtt/connect", ConnectMqtt)
            .WithName("ConnectMqtt")
            .WithDescription("Connect to MQTT broker");

        group.MapPost("/servers/{serverId}/mqtt/disconnect", DisconnectMqtt)
            .WithName("DisconnectMqtt")
            .WithDescription("Disconnect from MQTT broker");

        // ==================== SCHEDULED TASKS ====================
        group.MapGet("/servers/{serverId}/tasks", GetScheduledTasks)
            .WithName("GetScheduledTasks")
            .WithDescription("Get scheduled tasks");

        group.MapPost("/servers/{serverId}/tasks/{taskName}/run", RunScheduledTask)
            .WithName("RunScheduledTask")
            .WithDescription("Run a scheduled task");

        group.MapPost("/servers/{serverId}/tasks/{taskName}/enable", EnableScheduledTask)
            .WithName("EnableScheduledTask")
            .WithDescription("Enable a scheduled task");

        group.MapPost("/servers/{serverId}/tasks/{taskName}/disable", DisableScheduledTask)
            .WithName("DisableScheduledTask")
            .WithDescription("Disable a scheduled task");

        // ==================== DISCORD ====================
        group.MapGet("/servers/{serverId}/discord/status", GetDiscordStatus)
            .WithName("GetDiscordStatus")
            .WithDescription("Get Discord bot status");

        group.MapPost("/servers/{serverId}/discord/test", TestDiscordNotification)
            .WithName("TestDiscordNotification")
            .WithDescription("Send test Discord notification");

        // ==================== CLI ====================
        group.MapGet("/servers/{serverId}/cli/commands", GetCliCommands)
            .WithName("GetCliCommands")
            .WithDescription("Get available CLI commands");

        group.MapPost("/servers/{serverId}/cli/execute", ExecuteCliCommand)
            .WithName("ExecuteCliCommand")
            .WithDescription("Execute a CLI command");

        // ==================== DEPENDENCIES ====================
        group.MapGet("/servers/{serverId}/dependencies", GetDependencyStatus)
            .WithName("GetDependencyStatus")
            .WithDescription("Get dependency status");

        // ==================== AUTO-SCALING ====================
        group.MapGet("/servers/{serverId}/scaling/status", GetAutoScalingStatus)
            .WithName("GetAutoScalingStatus")
            .WithDescription("Get auto-scaling configuration and status");

        group.MapPost("/servers/{serverId}/scaling/scale-up", ManualScaleUp)
            .WithName("ManualScaleUp")
            .WithDescription("Manually scale up (add server)");

        group.MapPost("/servers/{serverId}/scaling/scale-down", ManualScaleDown)
            .WithName("ManualScaleDown")
            .WithDescription("Manually scale down (remove server)");

        // ==================== TEMPLATES ====================
        group.MapGet("/servers/{serverId}/templates", GetTemplates)
            .WithName("GetTemplates")
            .WithDescription("Get server templates");

        group.MapPost("/servers/{serverId}/templates", CreateTemplate)
            .WithName("CreateTemplate")
            .WithDescription("Create a server template");

        group.MapGet("/servers/{serverId}/templates/{templateId}", GetTemplate)
            .WithName("GetTemplate")
            .WithDescription("Get a specific template");

        group.MapPut("/servers/{serverId}/templates/{templateId}", UpdateTemplate)
            .WithName("UpdateTemplate")
            .WithDescription("Update a template");

        group.MapDelete("/servers/{serverId}/templates/{templateId}", DeleteTemplate)
            .WithName("DeleteTemplate")
            .WithDescription("Delete a template");

        group.MapPost("/servers/{serverId}/templates/{templateId}/apply", ApplyTemplate)
            .WithName("ApplyTemplate")
            .WithDescription("Apply a template to create servers");

        // ==================== WEBHOOKS ====================
        group.MapGet("/servers/{serverId}/webhooks", GetWebhooks)
            .WithName("GetWebhooks")
            .WithDescription("Get registered webhooks");

        group.MapPost("/servers/{serverId}/webhooks", RegisterWebhook)
            .WithName("RegisterWebhook")
            .WithDescription("Register a new webhook");

        group.MapDelete("/servers/{serverId}/webhooks/{webhookId}", DeleteWebhook)
            .WithName("DeleteWebhook")
            .WithDescription("Delete a webhook");

        group.MapPost("/servers/{serverId}/webhooks/{webhookId}/test", TestWebhook)
            .WithName("TestWebhook")
            .WithDescription("Test a webhook");

        // ==================== NOTIFICATIONS ====================
        group.MapGet("/servers/{serverId}/notifications", GetNotifications)
            .WithName("GetNotifications")
            .WithDescription("Get recent notifications");

        group.MapGet("/servers/{serverId}/notifications/unacknowledged", GetUnacknowledgedNotifications)
            .WithName("GetUnacknowledgedNotifications")
            .WithDescription("Get unacknowledged notifications");

        group.MapPost("/servers/{serverId}/notifications/{notificationId}/acknowledge", AcknowledgeNotification)
            .WithName("AcknowledgeNotification")
            .WithDescription("Acknowledge a notification");

        group.MapDelete("/servers/{serverId}/notifications", ClearNotifications)
            .WithName("ClearNotifications")
            .WithDescription("Clear all notifications");

        group.MapGet("/servers/{serverId}/notifications/thresholds", GetAlertThresholds)
            .WithName("GetAlertThresholds")
            .WithDescription("Get alert thresholds");

        group.MapPut("/servers/{serverId}/notifications/thresholds", UpdateAlertThresholds)
            .WithName("UpdateAlertThresholds")
            .WithDescription("Update alert thresholds");

        // ==================== CHARTS ====================
        group.MapGet("/servers/{serverId}/charts/uptime", GetUptimeChart)
            .WithName("GetUptimeChart")
            .WithDescription("Get uptime chart data");

        group.MapGet("/servers/{serverId}/charts/players", GetPlayersChart)
            .WithName("GetPlayersChart")
            .WithDescription("Get player count chart data");

        group.MapGet("/servers/{serverId}/charts/matches", GetMatchesChart)
            .WithName("GetMatchesChart")
            .WithDescription("Get matches chart data");

        // ==================== ADVANCED METRICS ====================
        group.MapGet("/servers/{serverId}/metrics/advanced/server/{instanceId}", GetServerMetrics)
            .WithName("GetServerMetrics")
            .WithDescription("Get server-specific metrics");

        group.MapGet("/servers/{serverId}/metrics/advanced/system", GetSystemMetricsHistory)
            .WithName("GetSystemMetricsHistory")
            .WithDescription("Get system metrics history");

        group.MapGet("/servers/{serverId}/metrics/advanced/summary", GetAllServersSummary)
            .WithName("GetAllServersSummary")
            .WithDescription("Get all servers summary");

        // ==================== REPLAY UPLOAD ====================
        group.MapGet("/servers/{serverId}/replays/upload", GetUploadedReplays)
            .WithName("GetUploadedReplays")
            .WithDescription("Get uploaded replays");

        group.MapGet("/servers/{serverId}/replays/upload/info/{fileName}", GetReplayInfo)
            .WithName("GetReplayInfo")
            .WithDescription("Get replay info");

        group.MapPost("/servers/{serverId}/replays/upload/process-pending", ProcessPendingUploads)
            .WithName("ProcessPendingUploads")
            .WithDescription("Process pending replay uploads");

        // ==================== MATCH STATS ====================
        group.MapPost("/servers/{serverId}/matchstats/resubmit", ResubmitPendingStats)
            .WithName("ResubmitPendingStats")
            .WithDescription("Resubmit pending match stats");

        // ==================== EVENTS EXTENDED ====================
        group.MapGet("/servers/{serverId}/events/export/json", ExportEventsJson)
            .WithName("ExportEventsJson")
            .WithDescription("Export events as JSON");

        group.MapGet("/servers/{serverId}/events/export/csv", ExportEventsCsv)
            .WithName("ExportEventsCsv")
            .WithDescription("Export events as CSV");

        group.MapPost("/servers/{serverId}/events/simulate", SimulateEvent)
            .WithName("SimulateEvent")
            .WithDescription("Simulate an event for testing");

        // ==================== HEALTH EXTENDED ====================
        group.MapGet("/servers/{serverId}/health/lag", GetLagCheck)
            .WithName("GetLagCheck")
            .WithDescription("Get network lag check");

        group.MapGet("/servers/{serverId}/health/installation", GetInstallationCheck)
            .WithName("GetInstallationCheck")
            .WithDescription("Check HoN installation");

        group.MapGet("/servers/{serverId}/health/autoping", CheckAutoPingHealth)
            .WithName("CheckAutoPingHealth")
            .WithDescription("Check AutoPing health");

        // Health check (public)
        group.MapGet("/health", () => Results.Ok(new { status = "healthy", timestamp = DateTime.UtcNow }))
            .WithName("HealthCheck");

        // Auto-registration from HoNfigurator (public endpoint - uses Discord User ID for auth)
        group.MapPost("/auto-register", AutoRegisterServer)
            .WithName("AutoRegisterServer")
            .WithDescription("Auto-register a server from HoNfigurator using Discord User ID");

        // Regenerate API Key
        group.MapPost("/my-servers/{serverId}/regenerate-key", RegenerateApiKey)
            .WithName("RegenerateApiKey")
            .WithDescription("Regenerate API key for a server");

        // Server Access Management (sharing with other Discord users)
        group.MapGet("/my-servers/{serverId}/access", GetServerAccess)
            .WithName("GetServerAccess")
            .WithDescription("Get list of users with access to this server");

        group.MapPost("/my-servers/{serverId}/access", AddServerAccess)
            .WithName("AddServerAccess")
            .WithDescription("Grant access to a Discord user");

        group.MapPut("/my-servers/{serverId}/access/{accessId}", UpdateServerAccess)
            .WithName("UpdateServerAccess")
            .WithDescription("Update user's access role");

        group.MapDelete("/my-servers/{serverId}/access/{accessId}", RemoveServerAccess)
            .WithName("RemoveServerAccess")
            .WithDescription("Remove user's access to this server");

        // User profile (get current user info)
        group.MapGet("/me", GetCurrentUserProfile)
            .WithName("GetCurrentUserProfile")
            .WithDescription("Get current authenticated user info");

        // SuperAdmin Management (only SuperAdmins can manage)
        group.MapGet("/admin/users", GetAllUsers)
            .WithName("GetAllUsers")
            .WithDescription("Get all users (SuperAdmin only)");

        group.MapPut("/admin/users/{userId}/superadmin", SetSuperAdmin)
            .WithName("SetSuperAdmin")
            .WithDescription("Set or remove SuperAdmin status for a user");

        // ==================== DISCORD EXTENDED ====================
        group.MapPut("/servers/{serverId}/discord/settings", UpdateDiscordSettings)
            .WithName("UpdateDiscordSettings")
            .WithDescription("Update Discord bot settings");

        group.MapPost("/servers/{serverId}/discord/test/match-start", TestMatchStartNotification)
            .WithName("TestMatchStartNotification")
            .WithDescription("Test match start notification");

        group.MapPost("/servers/{serverId}/discord/test/match-end", TestMatchEndNotification)
            .WithName("TestMatchEndNotification")
            .WithDescription("Test match end notification");

        group.MapPost("/servers/{serverId}/discord/test/player-join", TestPlayerJoinNotification)
            .WithName("TestPlayerJoinNotification")
            .WithDescription("Test player join notification");

        group.MapPost("/servers/{serverId}/discord/test/alert", TestAlertNotification)
            .WithName("TestAlertNotification")
            .WithDescription("Test alert notification");

        // ==================== STATISTICS EXTENDED ====================
        group.MapGet("/servers/{serverId}/statistics/players/{playerName}", GetPlayerStats)
            .WithName("GetPlayerStats")
            .WithDescription("Get statistics for a specific player");

        group.MapGet("/servers/{serverId}/statistics/matches/{matchId}", GetMatchDetails)
            .WithName("GetMatchDetails")
            .WithDescription("Get details of a specific match");

        group.MapGet("/servers/{serverId}/statistics/servers", GetAllServerStats)
            .WithName("GetAllServerStats")
            .WithDescription("Get statistics for all servers");

        // ==================== EVENTS EXTENDED ====================
        group.MapGet("/servers/{serverId}/events/server/{instanceId}", GetEventsByServer)
            .WithName("GetEventsByServer")
            .WithDescription("Get events for a specific server instance");

        group.MapGet("/servers/{serverId}/events/type/{eventType}", GetEventsByType)
            .WithName("GetEventsByType")
            .WithDescription("Get events filtered by type");

        group.MapGet("/servers/{serverId}/events/mqtt-publishable", GetMqttPublishableEvents)
            .WithName("GetMqttPublishableEvents")
            .WithDescription("Get events that can be published to MQTT");

        // ==================== CHARTS EXTENDED ====================
        group.MapGet("/servers/{serverId}/charts/uptime/{instanceId}", GetServerUptimeChart)
            .WithName("GetServerUptimeChart")
            .WithDescription("Get uptime chart for a specific server instance");

        group.MapGet("/servers/{serverId}/charts/resources", GetResourceCharts)
            .WithName("GetResourceCharts")
            .WithDescription("Get resource usage charts (CPU, Memory, Disk)");

        group.MapGet("/servers/{serverId}/charts/matches/summary", GetMatchSummary)
            .WithName("GetMatchSummary")
            .WithDescription("Get match statistics summary");

        // ==================== ADVANCED METRICS EXTENDED ====================
        group.MapGet("/servers/{serverId}/metrics/advanced/compare", CompareServers)
            .WithName("CompareServers")
            .WithDescription("Compare metrics between servers");

        // ==================== KICK PLAYER ====================
        group.MapPost("/servers/{serverId}/instances/{instanceId}/kick", KickPlayer)
            .WithName("KickPlayer")
            .WithDescription("Kick a player from a server instance");

        // ==================== FILEBEAT ====================
        group.MapGet("/servers/{serverId}/filebeat/status", GetFilebeatStatus)
            .WithName("GetFilebeatStatus")
            .WithDescription("Get Filebeat service status");

        group.MapPost("/servers/{serverId}/filebeat/install", InstallFilebeat)
            .WithName("InstallFilebeat")
            .WithDescription("Install Filebeat service");

        group.MapPost("/servers/{serverId}/filebeat/start", StartFilebeat)
            .WithName("StartFilebeat")
            .WithDescription("Start Filebeat service");

        group.MapPost("/servers/{serverId}/filebeat/stop", StopFilebeat)
            .WithName("StopFilebeat")
            .WithDescription("Stop Filebeat service");

        group.MapPost("/servers/{serverId}/filebeat/configure", ConfigureFilebeat)
            .WithName("ConfigureFilebeat")
            .WithDescription("Configure Filebeat settings");

        group.MapPost("/servers/{serverId}/filebeat/test", TestFilebeat)
            .WithName("TestFilebeat")
            .WithDescription("Test Filebeat connection");

        // ==================== RBAC (Role-Based Access Control) ====================
        group.MapGet("/servers/{serverId}/rbac/permissions", GetRbacPermissions)
            .WithName("GetRbacPermissions")
            .WithDescription("Get all available permissions");

        group.MapGet("/servers/{serverId}/rbac/roles", GetRbacRoles)
            .WithName("GetRbacRoles")
            .WithDescription("Get all roles");

        group.MapPost("/servers/{serverId}/rbac/roles", CreateRbacRole)
            .WithName("CreateRbacRole")
            .WithDescription("Create a new role");

        group.MapGet("/servers/{serverId}/rbac/roles/{roleName}", GetRbacRole)
            .WithName("GetRbacRole")
            .WithDescription("Get a specific role");

        group.MapPut("/servers/{serverId}/rbac/roles/{roleName}", UpdateRbacRole)
            .WithName("UpdateRbacRole")
            .WithDescription("Update a role");

        group.MapDelete("/servers/{serverId}/rbac/roles/{roleName}", DeleteRbacRole)
            .WithName("DeleteRbacRole")
            .WithDescription("Delete a role");

        group.MapPut("/servers/{serverId}/rbac/roles/{roleName}/permissions", UpdateRbacRolePermissions)
            .WithName("UpdateRbacRolePermissions")
            .WithDescription("Update role permissions");

        group.MapGet("/servers/{serverId}/rbac/users/{userId}/permissions", GetUserRbacPermissions)
            .WithName("GetUserRbacPermissions")
            .WithDescription("Get user's effective permissions");

        group.MapGet("/servers/{serverId}/rbac/users/{userId}/roles", GetUserRbacRoles)
            .WithName("GetUserRbacRoles")
            .WithDescription("Get user's assigned roles");

        group.MapPut("/servers/{serverId}/rbac/users/{userId}/roles", UpdateUserRbacRoles)
            .WithName("UpdateUserRbacRoles")
            .WithDescription("Update user's role assignments");

        // ==================== DIAGNOSTICS (Skipped Frames) ====================
        group.MapGet("/servers/{serverId}/diagnostics/skipped-frames", GetSkippedFrames)
            .WithName("GetSkippedFrames")
            .WithDescription("Get skipped frames diagnostics summary");

        group.MapGet("/servers/{serverId}/diagnostics/skipped-frames/server/{instanceId}", GetSkippedFramesByServer)
            .WithName("GetSkippedFramesByServer")
            .WithDescription("Get skipped frames for a specific server instance");

        group.MapGet("/servers/{serverId}/diagnostics/skipped-frames/player/{playerName}", GetSkippedFramesByPlayer)
            .WithName("GetSkippedFramesByPlayer")
            .WithDescription("Get skipped frames for a specific player");

        group.MapPost("/servers/{serverId}/diagnostics/skipped-frames/reset", ResetSkippedFrames)
            .WithName("ResetSkippedFrames")
            .WithDescription("Reset skipped frames data");

        // ==================== STORAGE ====================
        group.MapGet("/servers/{serverId}/storage/status", GetStorageStatus)
            .WithName("GetStorageStatus")
            .WithDescription("Get storage status and disk usage");

        group.MapGet("/servers/{serverId}/storage/analytics", GetStorageAnalytics)
            .WithName("GetStorageAnalytics")
            .WithDescription("Get storage analytics and trends");

        group.MapPost("/servers/{serverId}/storage/relocate", RelocateStorage)
            .WithName("RelocateStorage")
            .WithDescription("Relocate storage to a new path");

        group.MapPost("/servers/{serverId}/storage/cleanup", CleanupStorage)
            .WithName("CleanupStorage")
            .WithDescription("Clean up unused storage");

        group.MapPost("/servers/{serverId}/storage/relocate-logs", RelocateLogs)
            .WithName("RelocateLogs")
            .WithDescription("Relocate log files to a new path");

        // ==================== GIT ====================
        group.MapGet("/servers/{serverId}/git/branch", GetCurrentBranch)
            .WithName("GetCurrentBranch")
            .WithDescription("Get current Git branch");

        group.MapGet("/servers/{serverId}/git/branches", GetBranches)
            .WithName("GetBranches")
            .WithDescription("Get all available Git branches");

        group.MapPost("/servers/{serverId}/git/switch/{branchName}", SwitchBranch)
            .WithName("SwitchBranch")
            .WithDescription("Switch to a different Git branch");

        group.MapGet("/servers/{serverId}/git/updates", GetGitUpdates)
            .WithName("GetGitUpdates")
            .WithDescription("Check for available Git updates");

        group.MapPost("/servers/{serverId}/git/pull", PullGitUpdates)
            .WithName("PullGitUpdates")
            .WithDescription("Pull latest Git updates");

        group.MapGet("/servers/{serverId}/git/version", GetGitVersion)
            .WithName("GetGitVersion")
            .WithDescription("Get current Git version/commit info");

        // ==================== SERVER SCALING SERVICE ====================
        group.MapGet("/servers/{serverId}/server-scaling/status", GetServerScalingStatus)
            .WithName("GetServerScalingStatus")
            .WithDescription("Get server scaling service status");

        group.MapPost("/servers/{serverId}/server-scaling/add/{count}", AddScalingServers)
            .WithName("AddScalingServers")
            .WithDescription("Add servers through scaling service");

        group.MapPost("/servers/{serverId}/server-scaling/remove/{count}", RemoveScalingServers)
            .WithName("RemoveScalingServers")
            .WithDescription("Remove servers through scaling service");

        group.MapPost("/servers/{serverId}/server-scaling/scale-to/{count}", ScaleToServerCount)
            .WithName("ScaleToServerCount")
            .WithDescription("Scale to specific server count");

        group.MapPost("/servers/{serverId}/server-scaling/auto-balance", AutoBalanceServers)
            .WithName("AutoBalanceServers")
            .WithDescription("Auto-balance server distribution");

        // ==================== BACKUPS ====================
        group.MapGet("/servers/{serverId}/backups", GetBackups)
            .WithName("GetBackups")
            .WithDescription("Get list of backups");

        group.MapPost("/servers/{serverId}/backups", CreateBackup)
            .WithName("CreateBackup")
            .WithDescription("Create a new backup");

        group.MapGet("/servers/{serverId}/backups/{backupId}", GetBackupDetails)
            .WithName("GetBackupDetails")
            .WithDescription("Get backup details");

        group.MapDelete("/servers/{serverId}/backups/{backupId}", DeleteBackup)
            .WithName("DeleteBackup")
            .WithDescription("Delete a backup");

        group.MapPost("/servers/{serverId}/backups/{backupId}/restore", RestoreBackup)
            .WithName("RestoreBackup")
            .WithDescription("Restore from a backup");

        group.MapGet("/servers/{serverId}/backups/{backupId}/download", DownloadBackup)
            .WithName("DownloadBackup")
            .WithDescription("Download a backup file");

        // ==================== MQTT EXTENDED ====================
        group.MapPost("/servers/{serverId}/mqtt/publish", PublishMqttMessage)
            .WithName("PublishMqttMessage")
            .WithDescription("Publish a message to MQTT");

        group.MapPost("/servers/{serverId}/mqtt/publish-test", PublishMqttTestMessage)
            .WithName("PublishMqttTestMessage")
            .WithDescription("Publish a test message to MQTT");

        // ==================== PERFORMANCE EXTENDED ====================
        group.MapGet("/servers/{serverId}/performance/servers", GetPerformanceServers)
            .WithName("GetPerformanceServers")
            .WithDescription("Get performance data for all server instances");

        // ==================== HEALTH EXTENDED ====================
        group.MapGet("/servers/{serverId}/health/ip/{ipAddress}", ValidateHealthIp)
            .WithName("ValidateHealthIp")
            .WithDescription("Validate health check from specific IP address");

        // ==================== CONSOLE COMMAND ====================
        group.MapPost("/servers/{serverId}/console/execute", ExecuteConsoleCommand)
            .WithName("ExecuteConsoleCommand")
            .WithDescription("Execute a console command on the server");

        // ==================== SYSTEM STATISTICS ====================
        group.MapGet("/servers/{serverId}/system/stats", GetSystemStats)
            .WithName("GetSystemStats")
            .WithDescription("Get system statistics");

        // ==================== VERSION INFO ====================
        group.MapGet("/servers/{serverId}/version", GetServerVersion)
            .WithName("GetServerVersion")
            .WithDescription("Get server version information");

        // ==================== REPLAY UPLOAD MANAGEMENT ====================
        group.MapPost("/servers/{serverId}/replays/upload/{matchId}", UploadReplayFile)
            .WithName("UploadReplayFile")
            .WithDescription("Upload a replay file for a specific match");

        group.MapDelete("/servers/{serverId}/replays/upload/{fileName}", DeleteUploadedReplay)
            .WithName("DeleteUploadedReplay")
            .WithDescription("Delete an uploaded replay file");

        group.MapGet("/servers/{serverId}/replays/upload/stats", GetReplayUploadStats)
            .WithName("GetReplayUploadStats")
            .WithDescription("Get replay upload statistics");

        // ==================== PUBLIC INFO PROXIES ====================
        group.MapGet("/servers/{serverId}/public/server-info", GetPublicServerInfo)
            .WithName("GetPublicServerInfo")
            .WithDescription("Get public server info");

        group.MapGet("/servers/{serverId}/public/hon-version", GetPublicHonVersion)
            .WithName("GetPublicHonVersion")
            .WithDescription("Get public HoN version");

        group.MapGet("/servers/{serverId}/public/skipped-frames/{port}", GetPublicSkippedFrameData)
            .WithName("GetPublicSkippedFrameData")
            .WithDescription("Get public skipped frame data");
    }

    /// <summary>
    /// Get current authenticated user info
    /// </summary>
    private static async Task<IResult> GetCurrentUserProfile(
        HttpContext httpContext,
        PortalDbContext db)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null)
        {
            return Results.Unauthorized();
        }

        return Results.Ok(new
        {
            user.Id,
            user.DiscordId,
            user.Username,
            AvatarUrl = user.GetAvatarUrl(),
            user.IsSuperAdmin,
            user.CreatedAt
        });
    }

    /// <summary>
    /// Get all users (SuperAdmin only)
    /// </summary>
    private static async Task<IResult> GetAllUsers(
        HttpContext httpContext,
        PortalDbContext db)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null)
        {
            return Results.Unauthorized();
        }

        if (!user.IsSuperAdmin)
        {
            return Results.Forbid();
        }

        var users = await db.Users
            .OrderByDescending(u => u.IsSuperAdmin)
            .ThenBy(u => u.Username)
            .ToListAsync();
        
        var result = users.Select(u => new
        {
            u.Id,
            u.DiscordId,
            u.Username,
            AvatarUrl = u.GetAvatarUrl(),
            u.IsSuperAdmin,
            u.CreatedAt,
            u.LastLoginAt,
            ServerCount = u.Servers.Count
        });

        return Results.Ok(result);
    }

    /// <summary>
    /// Set SuperAdmin status for a user
    /// </summary>
    private static async Task<IResult> SetSuperAdmin(
        int userId,
        SetSuperAdminRequest request,
        HttpContext httpContext,
        PortalDbContext db)
    {
        var currentUser = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (currentUser == null)
        {
            return Results.Unauthorized();
        }

        if (!currentUser.IsSuperAdmin)
        {
            return Results.Forbid();
        }

        // Prevent removing your own SuperAdmin status
        if (userId == currentUser.Id && !request.IsSuperAdmin)
        {
            return Results.BadRequest(new { error = "Cannot remove your own SuperAdmin status" });
        }

        var targetUser = await db.Users.FindAsync(userId);
        if (targetUser == null)
        {
            return Results.NotFound(new { error = "User not found" });
        }

        targetUser.IsSuperAdmin = request.IsSuperAdmin;
        await db.SaveChangesAsync();

        return Results.Ok(new
        {
            targetUser.Id,
            targetUser.Username,
            targetUser.IsSuperAdmin
        });
    }

    /// <summary>
    /// Get all servers belonging to the current user (owned + shared)
    /// </summary>
    private static async Task<IResult> GetMyServers(
        HttpContext httpContext,
        PortalDbContext db,
        ServerStatusService statusService)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null)
        {
            return Results.Unauthorized();
        }

        var result = new List<object>();

        // SuperAdmin can see ALL servers
        if (user.IsSuperAdmin)
        {
            var allServers = await db.Servers
                .Include(s => s.Owner)
                .OrderBy(s => s.ServerName)
                .ToListAsync();

            foreach (var s in allServers)
            {
                var status = statusService.GetStatus(s.ServerId);
                var isOwner = s.OwnerId == user.Id;
                result.Add(new
                {
                    s.Id,
                    s.ServerId,
                    s.ServerName,
                    s.IpAddress,
                    s.ApiPort,
                    s.Region,
                    s.Version,
                    s.IsOnline,
                    s.CreatedAt,
                    s.LastSeenAt,
                    ApiKey = isOwner ? s.ApiKey : null, // Only show API key if owner
                    Status = status,
                    Role = isOwner ? "Owner" : "SuperAdmin",
                    IsOwner = isOwner,
                    IsSuperAdmin = true,
                    OwnerName = s.Owner?.Username
                });
            }

            return Results.Ok(result);
        }

        // Get owned servers
        var ownedServers = await db.Servers
            .Where(s => s.OwnerId == user.Id)
            .OrderBy(s => s.ServerName)
            .ToListAsync();

        // Get servers shared with this user (by Discord ID or User ID)
        var sharedAccess = await db.ServerAccess
            .Include(a => a.Server)
            .ThenInclude(s => s!.Owner)
            .Where(a => a.DiscordId == user.DiscordId || a.UserId == user.Id)
            .ToListAsync();

        // Add owned servers
        foreach (var s in ownedServers)
        {
            var status = statusService.GetStatus(s.ServerId);
            result.Add(new
            {
                s.Id,
                s.ServerId,
                s.ServerName,
                s.IpAddress,
                s.ApiPort,
                s.Region,
                s.Version,
                s.IsOnline,
                s.CreatedAt,
                s.LastSeenAt,
                s.ApiKey,
                Status = status,
                Role = "Owner",
                IsOwner = true,
                IsSuperAdmin = false,
                OwnerName = user.Username
            });
        }

        // Add shared servers
        foreach (var access in sharedAccess)
        {
            var s = access.Server!;
            var status = statusService.GetStatus(s.ServerId);
            // Owner role can see API key
            var canSeeApiKey = access.Role == ServerRole.Owner;
            result.Add(new
            {
                s.Id,
                s.ServerId,
                s.ServerName,
                s.IpAddress,
                s.ApiPort,
                s.Region,
                s.Version,
                s.IsOnline,
                s.CreatedAt,
                s.LastSeenAt,
                ApiKey = canSeeApiKey ? s.ApiKey : null,
                Status = status,
                Role = access.Role.ToString(),
                IsOwner = false,
                IsSuperAdmin = false,
                OwnerName = s.Owner?.Username
            });
        }

        return Results.Ok(result.OrderBy(r => ((dynamic)r).ServerName));
    }

    /// <summary>
    /// Add a new server host
    /// </summary>
    private static async Task<IResult> AddServer(
        AddServerRequest request,
        HttpContext httpContext,
        PortalDbContext db,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null)
        {
            return Results.Unauthorized();
        }

        // Validate request
        if (string.IsNullOrWhiteSpace(request.ServerName))
        {
            return Results.BadRequest(new { error = "Server name is required" });
        }
        if (string.IsNullOrWhiteSpace(request.IpAddress))
        {
            return Results.BadRequest(new { error = "IP address is required" });
        }

        // Check if IP already exists for this user
        var existingIp = await db.Servers
            .AnyAsync(s => s.OwnerId == user.Id && s.IpAddress == request.IpAddress);
        if (existingIp)
        {
            return Results.BadRequest(new { error = "This IP address is already registered" });
        }

        var server = new RegisteredServer
        {
            OwnerId = user.Id,
            ServerId = Guid.NewGuid().ToString("N")[..12].ToUpper(),
            ServerName = request.ServerName,
            IpAddress = request.IpAddress,
            ApiPort = request.ApiPort > 0 ? request.ApiPort : 5050,
            Region = request.Region ?? "Unknown",
            ServerUrl = $"http://{request.IpAddress}:{(request.ApiPort > 0 ? request.ApiPort : 5050)}",
            ApiKey = GenerateApiKey(),
            CreatedAt = DateTime.UtcNow,
            LastSeenAt = DateTime.MinValue,
            IsOnline = false
        };

        db.Servers.Add(server);
        await db.SaveChangesAsync();

        logger.LogInformation("User {Username} added server: {ServerName} ({IpAddress})", 
            user.Username, server.ServerName, server.IpAddress);

        return Results.Ok(new
        {
            server.Id,
            server.ServerId,
            server.ServerName,
            server.IpAddress,
            server.ApiPort,
            server.Region,
            server.ApiKey,
            message = "Server added successfully. Configure HoNfigurator with this API key to enable status reporting."
        });
    }

    /// <summary>
    /// Update server details
    /// </summary>
    private static async Task<IResult> UpdateServer(
        string serverId,
        UpdateServerRequest request,
        HttpContext httpContext,
        PortalDbContext db,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null)
        {
            return Results.Unauthorized();
        }

        var server = await db.Servers
            .FirstOrDefaultAsync(s => s.ServerId == serverId && s.OwnerId == user.Id);

        if (server == null)
        {
            return Results.NotFound(new { error = "Server not found" });
        }

        // Update fields if provided
        if (!string.IsNullOrWhiteSpace(request.ServerName))
        {
            server.ServerName = request.ServerName;
        }
        if (!string.IsNullOrWhiteSpace(request.IpAddress))
        {
            // Check if new IP already exists for this user
            var existingIp = await db.Servers
                .AnyAsync(s => s.OwnerId == user.Id && s.IpAddress == request.IpAddress && s.Id != server.Id);
            if (existingIp)
            {
                return Results.BadRequest(new { error = "This IP address is already registered" });
            }
            server.IpAddress = request.IpAddress;
        }
        if (request.ApiPort.HasValue && request.ApiPort.Value > 0)
        {
            server.ApiPort = request.ApiPort.Value;
        }
        if (!string.IsNullOrWhiteSpace(request.Region))
        {
            server.Region = request.Region;
        }

        // Update URL
        server.ServerUrl = $"http://{server.IpAddress}:{server.ApiPort}";

        await db.SaveChangesAsync();

        logger.LogInformation("User {Username} updated server: {ServerId}", user.Username, serverId);

        return Results.Ok(new
        {
            server.Id,
            server.ServerId,
            server.ServerName,
            server.IpAddress,
            server.ApiPort,
            server.Region,
            message = "Server updated successfully"
        });
    }

    /// <summary>
    /// Delete a server
    /// </summary>
    private static async Task<IResult> DeleteServer(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        ServerStatusService statusService,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null)
        {
            return Results.Unauthorized();
        }

        var server = await db.Servers
            .FirstOrDefaultAsync(s => s.ServerId == serverId);

        if (server == null)
        {
            return Results.NotFound(new { error = "Server not found" });
        }

        // Check permission: Original Owner, Owner role, or SuperAdmin
        var isOriginalOwner = server.OwnerId == user.Id;
        var hasOwnerRole = await db.ServerAccess
            .AnyAsync(a => a.ServerId == server.Id 
                && (a.DiscordId == user.DiscordId || a.UserId == user.Id)
                && a.Role == ServerRole.Owner);
        
        if (!isOriginalOwner && !hasOwnerRole && !user.IsSuperAdmin)
        {
            return Results.Forbid();
        }

        db.Servers.Remove(server);
        await db.SaveChangesAsync();

        statusService.RemoveServer(serverId);

        logger.LogInformation("User {Username} deleted server: {ServerId}", user.Username, serverId);

        return Results.Ok(new { message = "Server deleted successfully" });
    }

    /// <summary>
    /// Get dashboard summary for current user's servers
    /// </summary>
    private static async Task<IResult> GetDashboard(
        HttpContext httpContext,
        PortalDbContext db,
        ServerStatusService statusService)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null)
        {
            return Results.Unauthorized();
        }

        var servers = await db.Servers
            .Where(s => s.OwnerId == user.Id)
            .ToListAsync();

        var summary = statusService.GetDashboardSummary(servers);
        return Results.Ok(summary);
    }

    /// <summary>
    /// Receive status report from HoNfigurator instance
    /// </summary>
    private static async Task<IResult> ReportStatus(
        ServerStatusReport report,
        PortalDbContext db,
        ServerStatusService statusService,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger,
        HttpContext httpContext)
    {
        // Validate API key
        var apiKey = httpContext.Request.Headers["X-Api-Key"].FirstOrDefault();
        if (string.IsNullOrEmpty(apiKey))
        {
            return Results.Unauthorized();
        }

        var server = await db.Servers
            .FirstOrDefaultAsync(s => s.ApiKey == apiKey);

        if (server == null)
        {
            logger.LogWarning("Status report from unknown API key");
            return Results.Unauthorized();
        }

        // Update server info from report
        server.IsOnline = true;
        server.LastSeenAt = DateTime.UtcNow;
        server.Version = report.Version;
        
        if (!string.IsNullOrEmpty(report.ServerName))
        {
            server.ServerName = report.ServerName;
        }

        await db.SaveChangesAsync();

        // Cache status in memory
        report.ServerId = server.ServerId;
        report.Timestamp = DateTime.UtcNow;
        statusService.UpdateStatus(server.ServerId, report);

        // Broadcast to owner's dashboard (could filter by owner later)
        await hubContext.Clients.All.SendAsync("ServerStatusUpdated", server.ServerId, report);

        return Results.Ok();
    }

    /// <summary>
    /// Get detailed server info from remote HoNfigurator
    /// </summary>
    private static async Task<IResult> GetServerDetails(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        ServerStatusService statusService)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Viewer role can view server details
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        // Get cached status
        var status = statusService.GetStatus(serverId);
        
        // Try to fetch live data from HoNfigurator
        try
        {
            var response = await _proxyClient.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/status");
            if (response.IsSuccessStatusCode)
            {
                var json = await response.Content.ReadAsStringAsync();
                var liveData = System.Text.Json.JsonSerializer.Deserialize<ServerStatusReport>(json, 
                    new System.Text.Json.JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                
                // Update cache with live data
                if (liveData != null)
                {
                    liveData.ServerId = serverId;
                    liveData.Timestamp = DateTime.UtcNow;
                    statusService.UpdateStatus(serverId, liveData);
                }
                
                return Results.Ok(new
                {
                    server = new
                    {
                        server.ServerId,
                        server.ServerName,
                        server.IpAddress,
                        server.ApiPort,
                        server.Region,
                        server.Version,
                        server.IsOnline,
                        server.LastSeenAt
                    },
                    cachedStatus = status,
                    liveData
                });
            }
        }
        catch
        {
            // Fall back to cached data
        }

        return Results.Ok(new
        {
            server = new
            {
                server.ServerId,
                server.ServerName,
                server.IpAddress,
                server.ApiPort,
                server.Region,
                server.Version,
                server.IsOnline,
                server.LastSeenAt
            },
            cachedStatus = status,
            liveData = (object?)null
        });
    }

    /// <summary>
    /// Start a game server instance
    /// </summary>
    private static async Task<IResult> StartGameInstance(
        string serverId,
        int instanceId,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Operator role required to start/stop instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/{instanceId}/start",
                null);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) started instance {Instance} on {Server}", 
                    user.Username, role, instanceId, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("InstanceAction", serverId, instanceId, "started");
                return Results.Ok(new { message = $"Instance {instanceId} started" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed to start: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to start instance {Instance} on {Server}", instanceId, server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Stop a game server instance
    /// </summary>
    private static async Task<IResult> StopGameInstance(
        string serverId,
        int instanceId,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Operator role required to start/stop instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/{instanceId}/stop",
                null);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) stopped instance {Instance} on {Server}", 
                    user.Username, role, instanceId, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("InstanceAction", serverId, instanceId, "stopped");
                return Results.Ok(new { message = $"Instance {instanceId} stopped" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed to stop: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to stop instance {Instance} on {Server}", instanceId, server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Restart a game server instance
    /// </summary>
    private static async Task<IResult> RestartGameInstance(
        string serverId,
        int instanceId,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Operator role required to start/stop instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });
        if (server == null) return Results.NotFound(new { error = "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/{instanceId}/restart",
                null);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) restarted instance {Instance} on {Server}", 
                    user.Username, role, instanceId, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("InstanceAction", serverId, instanceId, "restarted");
                return Results.Ok(new { message = $"Instance {instanceId} restarted" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed to restart: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to restart instance {Instance} on {Server}", instanceId, server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Start all game server instances
    /// </summary>
    private static async Task<IResult> StartAllInstances(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Operator role required to start/stop instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/start-all",
                null);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) started all instances on {Server}", 
                    user.Username, role, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("BulkAction", serverId, "started-all");
                return Results.Ok(new { message = "All instances started" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to start all instances on {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Stop all game server instances
    /// </summary>
    private static async Task<IResult> StopAllInstances(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Operator role required to start/stop instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/stop-all",
                null);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) stopped all instances on {Server}", 
                    user.Username, role, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("BulkAction", serverId, "stopped-all");
                return Results.Ok(new { message = "All instances stopped" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to stop all instances on {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Restart all game server instances
    /// </summary>
    private static async Task<IResult> RestartAllInstances(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Operator role required to start/stop instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/restart-all",
                null);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) restarted all instances on {Server}", 
                    user.Username, role, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("BulkAction", serverId, "restarted-all");
                return Results.Ok(new { message = "All instances restarted" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to restart all instances on {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Scale instances to target count
    /// </summary>
    private static async Task<IResult> ScaleInstances(
        string serverId,
        ScaleRequest request,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Admin role required to scale instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsJsonAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/scale",
                new { targetCount = request.TargetCount });

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) scaled to {Target} instances on {Server}", 
                    user.Username, role, request.TargetCount, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("BulkAction", serverId, "scaled");
                return Results.Ok(new { message = $"Scaled to {request.TargetCount} instances" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to scale instances on {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Add a new game server instance
    /// </summary>
    private static async Task<IResult> AddInstance(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Admin role required to add/delete instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsJsonAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/add",
                new { count = 1 });

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} added instance on {Server}", 
                    user.Username, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("BulkAction", serverId, "instance-added");
                return Results.Ok(new { message = "Instance added" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to add instance on {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Add all instances up to maximum CPU capacity
    /// </summary>
    private static async Task<IResult> AddAllInstances(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Admin role required to add instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/add-all",
                null);

            if (response.IsSuccessStatusCode)
            {
                var result = await response.Content.ReadFromJsonAsync<object>();
                logger.LogInformation("User {User} added all instances on {Server}", 
                    user.Username, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("BulkAction", serverId, "instances-added-all");
                return Results.Ok(result);
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to add all instances on {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Delete a game server instance
    /// </summary>
    private static async Task<IResult> DeleteInstance(
        string serverId,
        int instanceId,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Admin role required to add/delete instances
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.SendAsync(new HttpRequestMessage(HttpMethod.Delete, 
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/delete")
                { Content = JsonContent.Create(new { count = 1 }) });

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) deleted instance {Instance} on {Server}", 
                    user.Username, role, instanceId, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("InstanceAction", serverId, instanceId, "deleted");
                return Results.Ok(new { message = $"Instance {instanceId} deleted" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to delete instance {Instance} on {Server}", instanceId, server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Broadcast message to all players on server
    /// </summary>
    private static async Task<IResult> BroadcastMessage(
        string serverId,
        BroadcastRequest request,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Operator role required to broadcast messages
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.PostAsJsonAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/servers/message-all",
                new { message = request.Message });

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) broadcast message on {Server}: {Message}", 
                    user.Username, role, server.ServerName, request.Message);
                
                await hubContext.Clients.All.SendAsync("BroadcastSent", serverId, request.Message);
                return Results.Ok(new { message = "Broadcast sent" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to broadcast on {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Get server configuration from HoNfigurator
    /// </summary>
    private static async Task<IResult> GetServerConfig(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Viewer role can view config
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            var response = await _proxyClient.GetAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/config");

            if (response.IsSuccessStatusCode)
            {
                var json = await response.Content.ReadAsStringAsync();
                return Results.Content(json, "application/json");
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed to get config: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get config from {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Update server configuration
    /// </summary>
    private static async Task<IResult> UpdateServerConfig(
        string serverId,
        ConfigUpdateRequest request,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Admin role required to update config
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            // Build config update payload
            var configPayload = new Dictionary<string, object>();
            
            if (request.ProxyEnabled.HasValue)
                configPayload["man_enableProxy"] = request.ProxyEnabled.Value;
            
            if (request.BasePort.HasValue)
                configPayload["svr_starting_gamePort"] = request.BasePort.Value;
            
            if (request.MaxPlayers.HasValue)
                configPayload["svr_maxClients"] = request.MaxPlayers.Value;

            if (request.TotalServers.HasValue)
                configPayload["svr_total"] = request.TotalServers.Value;

            var response = await _proxyClient.PostAsJsonAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/config",
                configPayload);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) updated config on {Server}: {Config}", 
                    user.Username, role, server.ServerName, System.Text.Json.JsonSerializer.Serialize(configPayload));
                
                await hubContext.Clients.All.SendAsync("ConfigUpdated", serverId);
                return Results.Ok(new { message = "Configuration updated successfully" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to update config on {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    /// <summary>
    /// Update full server configuration
    /// </summary>
    private static async Task<IResult> UpdateServerConfigFull(
        string serverId,
        HttpRequest httpRequest,
        HttpContext httpContext,
        PortalDbContext db,
        IHubContext<PortalHub> hubContext,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Admin role required to update full config
        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (server == null) return Results.NotFound(new { error = error ?? "Server not found" });

        try
        {
            // Read the raw JSON body and forward it directly to HoNfigurator
            using var reader = new StreamReader(httpRequest.Body);
            var jsonBody = await reader.ReadToEndAsync();
            
            var content = new StringContent(jsonBody, System.Text.Encoding.UTF8, "application/json");
            var response = await _proxyClient.PostAsync(
                $"http://{server.IpAddress}:{server.ApiPort}/api/config",
                content);

            if (response.IsSuccessStatusCode)
            {
                logger.LogInformation("User {User} ({Role}) updated full config on {Server}", 
                    user.Username, role, server.ServerName);
                
                await hubContext.Clients.All.SendAsync("ConfigUpdated", serverId);
                return Results.Ok(new { message = "Full configuration updated successfully" });
            }

            var errorMsg = await response.Content.ReadAsStringAsync();
            return Results.BadRequest(new { error = $"Failed: {errorMsg}" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to update full config on {Server}", server.ServerName);
            return Results.BadRequest(new { error = "Connection failed - server may be offline" });
        }
    }

    private static string GenerateApiKey()
    {
        var bytes = new byte[32];
        System.Security.Cryptography.RandomNumberGenerator.Fill(bytes);
        return Convert.ToBase64String(bytes).Replace("+", "-").Replace("/", "_").TrimEnd('=');
    }

    /// <summary>
    /// Auto-register a server from HoNfigurator using Discord User ID
    /// This allows HoNfigurator to register itself without manual API key setup
    /// </summary>
    private static async Task<IResult> AutoRegisterServer(
        AutoRegisterRequest request,
        HttpContext httpContext,
        PortalDbContext db,
        ILogger<Program> logger)
    {
        // Validate Discord User ID
        if (string.IsNullOrEmpty(request.DiscordUserId))
        {
            return Results.BadRequest(new { error = "Discord User ID is required" });
        }

        if (string.IsNullOrEmpty(request.ServerName) || string.IsNullOrEmpty(request.IpAddress))
        {
            return Results.BadRequest(new { error = "Server name and IP address are required" });
        }

        // Find or create user by Discord ID
        var user = await db.Users.FirstOrDefaultAsync(u => u.DiscordId == request.DiscordUserId);
        if (user == null)
        {
            // Auto-create user with Discord ID
            user = new PortalUser
            {
                DiscordId = request.DiscordUserId,
                Username = request.DiscordUsername ?? $"User_{request.DiscordUserId}",
                CreatedAt = DateTime.UtcNow
            };
            db.Users.Add(user);
            await db.SaveChangesAsync();
            logger.LogInformation("Auto-created user {Username} with Discord ID {DiscordId}", 
                user.Username, request.DiscordUserId);
        }

        // Check if server already exists for this IP
        var existingServer = await db.Servers.FirstOrDefaultAsync(s => s.IpAddress == request.IpAddress);
        
        if (existingServer != null)
        {
            // If same owner, return existing API key
            if (existingServer.OwnerId == user.Id)
            {
                logger.LogInformation("Server {ServerName} already registered, returning existing API key", 
                    existingServer.ServerName);
                
                // Update server info if changed
                existingServer.ServerName = request.ServerName;
                existingServer.ApiPort = request.ApiPort ?? existingServer.ApiPort;
                existingServer.Version = request.Version ?? existingServer.Version;
                existingServer.LastSeenAt = DateTime.UtcNow;
                await db.SaveChangesAsync();

                return Results.Ok(new AutoRegisterResponse
                {
                    Success = true,
                    Message = "Server already registered",
                    ServerId = existingServer.ServerId,
                    ApiKey = existingServer.ApiKey,
                    IsNewRegistration = false
                });
            }
            
            return Results.BadRequest(new { error = "This IP address is already registered by another user" });
        }

        // Create new server
        var serverId = Guid.NewGuid().ToString("N")[..12];
        var apiKey = GenerateApiKey();

        var server = new RegisteredServer
        {
            ServerId = serverId,
            ServerName = request.ServerName,
            IpAddress = request.IpAddress,
            ApiPort = request.ApiPort ?? 5050,
            Region = request.Region ?? "Unknown",
            Version = request.Version,
            ApiKey = apiKey,
            OwnerId = user.Id,
            IsOnline = true,
            CreatedAt = DateTime.UtcNow,
            LastSeenAt = DateTime.UtcNow
        };

        db.Servers.Add(server);
        await db.SaveChangesAsync();

        logger.LogInformation("Auto-registered server {ServerName} at {IpAddress} for user {Username}", 
            server.ServerName, server.IpAddress, user.Username);

        return Results.Ok(new AutoRegisterResponse
        {
            Success = true,
            Message = "Server registered successfully",
            ServerId = serverId,
            ApiKey = apiKey,
            IsNewRegistration = true
        });
    }

    /// <summary>
    /// Regenerate API key for a server
    /// </summary>
    private static async Task<IResult> RegenerateApiKey(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var server = await db.Servers
            .FirstOrDefaultAsync(s => s.ServerId == serverId && s.OwnerId == user.Id);
        if (server == null) return Results.NotFound(new { error = "Server not found" });

        var newApiKey = GenerateApiKey();
        server.ApiKey = newApiKey;
        await db.SaveChangesAsync();

        logger.LogInformation("Regenerated API key for server {ServerName}", server.ServerName);

        return Results.Ok(new { 
            message = "API key regenerated successfully", 
            apiKey = newApiKey 
        });
    }

    /// <summary>
    /// Get list of users with access to a server
    /// </summary>
    private static async Task<IResult> GetServerAccess(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Check if user is owner
        var server = await db.Servers
            .Include(s => s.SharedAccess)
            .FirstOrDefaultAsync(s => s.ServerId == serverId && s.OwnerId == user.Id);
        
        if (server == null) return Results.NotFound(new { error = "Server not found" });

        var accessList = new List<AccessEntryResponse>();

        foreach (var access in server.SharedAccess)
        {
            // Try to find registered user
            var registeredUser = access.UserId.HasValue 
                ? await db.Users.FindAsync(access.UserId.Value)
                : await db.Users.FirstOrDefaultAsync(u => u.DiscordId == access.DiscordId);

            accessList.Add(new AccessEntryResponse
            {
                Id = access.Id,
                DiscordId = access.DiscordId,
                Username = registeredUser?.Username,
                AvatarUrl = registeredUser?.GetAvatarUrl(),
                Role = access.Role,
                CreatedAt = access.CreatedAt,
                IsRegistered = registeredUser != null
            });
        }

        return Results.Ok(accessList.OrderBy(a => a.Role).ThenBy(a => a.CreatedAt));
    }

    /// <summary>
    /// Grant access to a Discord user
    /// </summary>
    private static async Task<IResult> AddServerAccess(
        string serverId,
        AddAccessRequest request,
        HttpContext httpContext,
        PortalDbContext db,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Check if user is owner
        var server = await db.Servers
            .FirstOrDefaultAsync(s => s.ServerId == serverId && s.OwnerId == user.Id);
        
        if (server == null) return Results.NotFound(new { error = "Server not found" });

        // Validate Discord ID
        if (string.IsNullOrWhiteSpace(request.DiscordId))
        {
            return Results.BadRequest(new { error = "Discord ID is required" });
        }

        // Check if access already exists
        var existingAccess = await db.ServerAccess
            .FirstOrDefaultAsync(a => a.ServerId == server.Id && a.DiscordId == request.DiscordId);
        
        if (existingAccess != null)
        {
            return Results.BadRequest(new { error = "User already has access to this server" });
        }

        // Check if target user is the owner
        if (request.DiscordId == user.DiscordId)
        {
            return Results.BadRequest(new { error = "You cannot add yourself as you are the owner" });
        }

        // Find if user is registered
        var targetUser = await db.Users.FirstOrDefaultAsync(u => u.DiscordId == request.DiscordId);

        var access = new ServerAccess
        {
            ServerId = server.Id,
            DiscordId = request.DiscordId,
            UserId = targetUser?.Id,
            Role = request.Role,
            CreatedAt = DateTime.UtcNow,
            GrantedById = user.Id
        };

        db.ServerAccess.Add(access);
        await db.SaveChangesAsync();

        logger.LogInformation("Granted {Role} access to Discord user {DiscordId} for server {ServerName} by {GrantedBy}",
            request.Role, request.DiscordId, server.ServerName, user.Username);

        return Results.Ok(new AccessEntryResponse
        {
            Id = access.Id,
            DiscordId = access.DiscordId,
            Username = targetUser?.Username,
            AvatarUrl = targetUser?.GetAvatarUrl(),
            Role = access.Role,
            CreatedAt = access.CreatedAt,
            IsRegistered = targetUser != null
        });
    }

    /// <summary>
    /// Update user's access role
    /// </summary>
    private static async Task<IResult> UpdateServerAccess(
        string serverId,
        int accessId,
        UpdateAccessRequest request,
        HttpContext httpContext,
        PortalDbContext db,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Check if user is owner
        var server = await db.Servers
            .FirstOrDefaultAsync(s => s.ServerId == serverId && s.OwnerId == user.Id);
        
        if (server == null) return Results.NotFound(new { error = "Server not found" });

        var access = await db.ServerAccess
            .FirstOrDefaultAsync(a => a.Id == accessId && a.ServerId == server.Id);
        
        if (access == null) return Results.NotFound(new { error = "Access entry not found" });

        access.Role = request.Role;
        await db.SaveChangesAsync();

        logger.LogInformation("Updated access role to {Role} for Discord user {DiscordId} on server {ServerName}",
            request.Role, access.DiscordId, server.ServerName);

        return Results.Ok(new { message = "Access updated successfully" });
    }

    /// <summary>
    /// Remove user's access to a server
    /// </summary>
    private static async Task<IResult> RemoveServerAccess(
        string serverId,
        int accessId,
        HttpContext httpContext,
        PortalDbContext db,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        // Check if user is owner
        var server = await db.Servers
            .FirstOrDefaultAsync(s => s.ServerId == serverId && s.OwnerId == user.Id);
        
        if (server == null) return Results.NotFound(new { error = "Server not found" });

        var access = await db.ServerAccess
            .FirstOrDefaultAsync(a => a.Id == accessId && a.ServerId == server.Id);
        
        if (access == null) return Results.NotFound(new { error = "Access entry not found" });

        db.ServerAccess.Remove(access);
        await db.SaveChangesAsync();

        logger.LogInformation("Removed access for Discord user {DiscordId} from server {ServerName}",
            access.DiscordId, server.ServerName);

        return Results.Ok(new { message = "Access removed successfully" });
    }

    // ==================== REPLAYS MANAGEMENT ====================

    /// <summary>
    /// Get list of replays from a server
    /// </summary>
    private static async Task<IResult> GetReplays(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error, replays = Array.Empty<object>() });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays");
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = "Failed to get replays", replays = Array.Empty<object>() });
            }

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get replays from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message, replays = Array.Empty<object>() });
        }
    }

    /// <summary>
    /// Get replay statistics from a server
    /// </summary>
    private static async Task<IResult> GetReplayStats(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays/manage/stats");
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = "Failed to get replay stats" });
            }

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get replay stats from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    /// <summary>
    /// Download a replay file from a server
    /// </summary>
    private static async Task<IResult> DownloadReplay(
        string serverId,
        string filename,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.NotFound(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(5); // Longer timeout for file downloads

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays/download/{filename}");
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.NotFound(new { error = "Replay not found" });
            }

            var bytes = await response.Content.ReadAsByteArrayAsync();
            return Results.File(bytes, "application/octet-stream", filename);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to download replay {Filename} from server {ServerId}", filename, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    /// <summary>
    /// Delete a replay file from a server
    /// </summary>
    private static async Task<IResult> DeleteReplayFile(
        string serverId,
        string filename,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.DeleteAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays/manage/{filename}");
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = "Failed to delete replay" });
            }

            return Results.Ok(new { message = "Replay deleted successfully" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to delete replay {Filename} from server {ServerId}", filename, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    /// <summary>
    /// Archive old replays on a server
    /// </summary>
    private static async Task<IResult> ArchiveReplays(
        string serverId,
        [FromQuery] int? daysOld,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(5);

            var days = daysOld ?? 30;
            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays/manage/archive?daysOld={days}", null);
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = "Failed to archive replays" });
            }

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to archive replays on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    /// <summary>
    /// Cleanup old archived replays on a server
    /// </summary>
    private static async Task<IResult> CleanupReplays(
        string serverId,
        [FromQuery] int? daysOld,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(5);

            var days = daysOld ?? 90;
            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays/manage/cleanup?daysOld={days}", null);
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = "Failed to cleanup replays" });
            }

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to cleanup replays on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== LOGS HANDLERS ====================

    private static async Task<IResult> GetServerLogs(
        string serverId,
        int instanceId,
        [FromQuery] int? lines,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var url = $"http://{server.IpAddress}:{server.ApiPort}/api/logs/{instanceId}";
            if (lines.HasValue) url += $"?lines={lines.Value}";
            
            var response = await client.GetAsync(url);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get logs", logs = "" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get logs from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message, logs = "" });
        }
    }

    private static async Task<IResult> DownloadServerLogs(
        string serverId,
        int instanceId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.NotFound(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(2);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/logs/{instanceId}/download");
            if (!response.IsSuccessStatusCode)
                return Results.NotFound(new { error = "Log file not found" });

            var bytes = await response.Content.ReadAsByteArrayAsync();
            return Results.File(bytes, "text/plain", $"server_{instanceId}.log");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to download logs from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== STATISTICS HANDLERS ====================

    private static async Task<IResult> GetStatisticsSummary(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/statistics/summary");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get statistics" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get statistics from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetRecentMatches(
        string serverId,
        [FromQuery] int? count,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error, matches = Array.Empty<object>() });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var url = $"http://{server.IpAddress}:{server.ApiPort}/api/statistics/matches";
            if (count.HasValue) url += $"?count={count.Value}";

            var response = await client.GetAsync(url);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get matches", matches = Array.Empty<object>() });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get matches from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message, matches = Array.Empty<object>() });
        }
    }

    private static async Task<IResult> GetTopPlayers(
        string serverId,
        [FromQuery] int? count,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error, players = Array.Empty<object>() });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var url = $"http://{server.IpAddress}:{server.ApiPort}/api/statistics/players/top";
            if (count.HasValue) url += $"?count={count.Value}";

            var response = await client.GetAsync(url);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get top players", players = Array.Empty<object>() });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get top players from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message, players = Array.Empty<object>() });
        }
    }

    private static async Task<IResult> GetMostActivePlayers(
        string serverId,
        [FromQuery] int? count,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error, players = Array.Empty<object>() });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var url = $"http://{server.IpAddress}:{server.ApiPort}/api/statistics/players/active";
            if (count.HasValue) url += $"?count={count.Value}";

            var response = await client.GetAsync(url);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get active players", players = Array.Empty<object>() });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get active players from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message, players = Array.Empty<object>() });
        }
    }

    private static async Task<IResult> GetDailyStats(
        string serverId,
        [FromQuery] int? days,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var url = $"http://{server.IpAddress}:{server.ApiPort}/api/statistics/daily";
            if (days.HasValue) url += $"?days={days.Value}";

            var response = await client.GetAsync(url);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get daily stats" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get daily stats from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== HEALTH HANDLERS ====================

    private static async Task<IResult> GetHealthChecks(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/health/checks");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get health checks" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get health checks from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetSystemResources(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/health/resources");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get system resources" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get system resources from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> RunHealthChecks(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(2);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/health/run", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to run health checks" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to run health checks on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetEnhancedHealthChecks(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/health/enhanced");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get enhanced health checks" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get enhanced health checks from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== METRICS HANDLERS ====================

    private static async Task<IResult> GetMetrics(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/metrics");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get metrics" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get metrics from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetMetricsHistory(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/metrics/history");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get metrics history" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get metrics history from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== PERFORMANCE HANDLERS ====================

    private static async Task<IResult> GetCurrentPerformance(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/performance/current");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get performance" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get performance from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetPerformanceHistory(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/performance/history");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get performance history" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get performance history from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetPerformanceSummary(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/performance/summary");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get performance summary" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get performance summary from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== PATCHING HANDLERS ====================

    private static async Task<IResult> GetPatchStatus(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/patching/status");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get patch status" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get patch status from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> CheckForPatches(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(2);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/patching/check");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to check for patches" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to check for patches on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ApplyPatch(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(30); // Patching can take a while

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/patching/apply", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to apply patch" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply patch on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== AUTOPING HANDLERS ====================

    private static async Task<IResult> GetAutoPingStatus(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/autoping/status");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get autoping status" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get autoping status from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> StartAutoPing(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/autoping/start", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to start autoping" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to start autoping on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> StopAutoPing(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/autoping/stop", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to stop autoping" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to stop autoping on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== EVENTS HANDLERS ====================

    private static async Task<IResult> GetEvents(
        string serverId,
        [FromQuery] int? count,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error, events = Array.Empty<object>() });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var url = $"http://{server.IpAddress}:{server.ApiPort}/api/events";
            if (count.HasValue) url += $"?count={count.Value}";

            var response = await client.GetAsync(url);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get events", events = Array.Empty<object>() });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get events from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message, events = Array.Empty<object>() });
        }
    }

    private static async Task<IResult> GetEventStats(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/events/stats");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get event stats" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get event stats from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== MQTT HANDLERS ====================

    private static async Task<IResult> GetMqttStatus(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/mqtt/status");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get MQTT status" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get MQTT status from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ConnectMqtt(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/mqtt/connect", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to connect MQTT" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to connect MQTT on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> DisconnectMqtt(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/mqtt/disconnect", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to disconnect MQTT" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to disconnect MQTT on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== SCHEDULED TASKS HANDLERS ====================

    private static async Task<IResult> GetScheduledTasks(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error, tasks = Array.Empty<object>() });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/tasks");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get scheduled tasks", tasks = Array.Empty<object>() });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get scheduled tasks from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message, tasks = Array.Empty<object>() });
        }
    }

    private static async Task<IResult> RunScheduledTask(
        string serverId,
        string taskName,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(5);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/tasks/{taskName}/run", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to run task" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to run task {TaskName} on server {ServerId}", taskName, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> EnableScheduledTask(
        string serverId,
        string taskName,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/tasks/{taskName}/enable", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to enable task" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to enable task {TaskName} on server {ServerId}", taskName, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> DisableScheduledTask(
        string serverId,
        string taskName,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/tasks/{taskName}/disable", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to disable task" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to disable task {TaskName} on server {ServerId}", taskName, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== DISCORD HANDLERS ====================

    private static async Task<IResult> GetDiscordStatus(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/discord/status");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get Discord status" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get Discord status from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> TestDiscordNotification(
        string serverId,
        [FromQuery] string? type,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var endpoint = type?.ToLower() switch
            {
                "match-start" => "/api/discord/test/match-start",
                "match-end" => "/api/discord/test/match-end",
                "player-join" => "/api/discord/test/player-join",
                _ => "/api/discord/test/alert"
            };

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}{endpoint}", null);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to send test notification" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to send test Discord notification on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== CLI HANDLERS ====================

    private static async Task<IResult> GetCliCommands(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error, commands = Array.Empty<object>() });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/cli/commands");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get CLI commands", commands = Array.Empty<object>() });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get CLI commands from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message, commands = Array.Empty<object>() });
        }
    }

    private static async Task<IResult> ExecuteCliCommand(
        string serverId,
        [FromBody] CliCommandRequest request,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(2);

            var json = System.Text.Json.JsonSerializer.Serialize(request);
            var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            
            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/cli/execute", content);
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to execute command" });

            var responseContent = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to execute CLI command on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== DEPENDENCIES HANDLERS ====================

    private static async Task<IResult> GetDependencyStatus(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/dependencies");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get dependency status" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get dependency status from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== AUTO-SCALING HANDLERS ====================

    private static async Task<IResult> GetAutoScalingStatus(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/scaling/status");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to get auto-scaling status" });

            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get auto-scaling status from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ManualScaleUp(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(60);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/scaling/scale-up", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Scale up failed" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to scale up server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ManualScaleDown(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(60);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/scaling/scale-down", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Scale down failed" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to scale down server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== TEMPLATES HANDLERS ====================

    private static async Task<IResult> GetTemplates(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/templates");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get templates from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> CreateTemplate(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            using var reader = new StreamReader(httpContext.Request.Body);
            var body = await reader.ReadToEndAsync();
            var content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/templates", content);
            var responseContent = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Failed to create template" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create template on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetTemplate(
        string serverId,
        string templateId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/templates/{templateId}");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.NotFound();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get template from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> UpdateTemplate(
        string serverId,
        string templateId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            using var reader = new StreamReader(httpContext.Request.Body);
            var body = await reader.ReadToEndAsync();
            var content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");

            var response = await client.PutAsync($"http://{server.IpAddress}:{server.ApiPort}/api/templates/{templateId}", content);
            var responseContent = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Failed to update template" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to update template on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> DeleteTemplate(
        string serverId,
        string templateId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.DeleteAsync($"http://{server.IpAddress}:{server.ApiPort}/api/templates/{templateId}");
            return response.IsSuccessStatusCode ? Results.Ok(new { message = "Template deleted" }) : Results.Json(new { error = "Failed to delete template" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to delete template on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ApplyTemplate(
        string serverId,
        string templateId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(60);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/templates/{templateId}/apply", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Failed to apply template" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply template on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== WEBHOOKS HANDLERS ====================

    private static async Task<IResult> GetWebhooks(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/webhooks");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get webhooks from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> RegisterWebhook(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            using var reader = new StreamReader(httpContext.Request.Body);
            var body = await reader.ReadToEndAsync();
            var content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/webhooks", content);
            var responseContent = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Failed to register webhook" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to register webhook on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> DeleteWebhook(
        string serverId,
        string webhookId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.DeleteAsync($"http://{server.IpAddress}:{server.ApiPort}/api/webhooks/{webhookId}");
            return response.IsSuccessStatusCode ? Results.Ok(new { message = "Webhook deleted" }) : Results.Json(new { error = "Failed to delete webhook" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to delete webhook on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> TestWebhook(
        string serverId,
        string webhookId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/webhooks/{webhookId}/test", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Failed to test webhook" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to test webhook on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== NOTIFICATIONS HANDLERS ====================

    private static async Task<IResult> GetNotifications(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger,
        int? count = null)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var url = $"http://{server.IpAddress}:{server.ApiPort}/api/notifications";
            if (count.HasValue) url += $"?count={count.Value}";

            var response = await client.GetAsync(url);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get notifications from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetUnacknowledgedNotifications(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/notifications/unacknowledged");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get unacknowledged notifications from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> AcknowledgeNotification(
        string serverId,
        string notificationId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/notifications/{notificationId}/acknowledge", null);
            return response.IsSuccessStatusCode ? Results.Ok(new { message = "Notification acknowledged" }) : Results.Json(new { error = "Failed to acknowledge" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to acknowledge notification on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ClearNotifications(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.DeleteAsync($"http://{server.IpAddress}:{server.ApiPort}/api/notifications");
            return response.IsSuccessStatusCode ? Results.Ok(new { message = "Notifications cleared" }) : Results.Json(new { error = "Failed to clear notifications" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to clear notifications on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetAlertThresholds(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/notifications/thresholds");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get alert thresholds from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> UpdateAlertThresholds(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            using var reader = new StreamReader(httpContext.Request.Body);
            var body = await reader.ReadToEndAsync();
            var content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");

            var response = await client.PutAsync($"http://{server.IpAddress}:{server.ApiPort}/api/notifications/thresholds", content);
            var responseContent = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Failed to update thresholds" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to update alert thresholds on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== CHARTS HANDLERS ====================

    private static async Task<IResult> GetUptimeChart(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/charts/uptime");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get uptime chart from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetPlayersChart(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/charts/players");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get players chart from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetMatchesChart(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/charts/matches");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get matches chart from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== ADVANCED METRICS HANDLERS ====================

    private static async Task<IResult> GetServerMetrics(
        string serverId,
        int instanceId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/metrics/advanced/server/{instanceId}");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get server metrics from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetSystemMetricsHistory(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/metrics/advanced/system");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get system metrics history from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetAllServersSummary(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/metrics/advanced/summary");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get all servers summary from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== REPLAY UPLOAD HANDLERS ====================

    private static async Task<IResult> GetUploadedReplays(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays/upload");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get uploaded replays from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetReplayInfo(
        string serverId,
        string fileName,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays/upload/info/{fileName}");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.NotFound();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get replay info from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ProcessPendingUploads(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(120);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays/upload/process-pending", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Failed to process pending uploads" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process pending uploads on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== MATCH STATS HANDLERS ====================

    private static async Task<IResult> ResubmitPendingStats(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(120);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/matchstats/resubmit", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Failed to resubmit pending stats" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to resubmit pending stats on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== EVENTS EXTENDED HANDLERS ====================

    private static async Task<IResult> ExportEventsJson(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(60);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/events/export/json");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to export events" });

            var content = await response.Content.ReadAsByteArrayAsync();
            return Results.File(content, "application/json", "events.json");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to export events from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ExportEventsCsv(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(60);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/events/export/csv");
            if (!response.IsSuccessStatusCode)
                return Results.Json(new { error = "Failed to export events" });

            var content = await response.Content.ReadAsByteArrayAsync();
            return Results.File(content, "text/csv", "events.csv");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to export events CSV from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> SimulateEvent(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Admin);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            using var reader = new StreamReader(httpContext.Request.Body);
            var body = await reader.ReadToEndAsync();
            var content = new StringContent(body, System.Text.Encoding.UTF8, "application/json");

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/events/simulate", content);
            var responseContent = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
            return response.IsSuccessStatusCode ? Results.Ok(data) : Results.Json(new { error = "Failed to simulate event" });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to simulate event on server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== HEALTH EXTENDED HANDLERS ====================

    private static async Task<IResult> GetLagCheck(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/health/lag");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get lag check from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetInstallationCheck(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/health/installation");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get installation check from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> CheckAutoPingHealth(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/autoping/health");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to check autoping health from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== DISCORD EXTENDED HANDLERS ====================

    private static async Task<IResult> UpdateDiscordSettings(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var requestBody = await new StreamReader(httpContext.Request.Body).ReadToEndAsync();
            var content = new StringContent(requestBody, System.Text.Encoding.UTF8, "application/json");
            var response = await client.PutAsync($"http://{server.IpAddress}:{server.ApiPort}/api/discord/settings", content);
            var responseContent = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to update Discord settings for server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> TestMatchStartNotification(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/discord/test/match-start", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to test match start notification for server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> TestMatchEndNotification(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/discord/test/match-end", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to test match end notification for server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> TestPlayerJoinNotification(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/discord/test/player-join", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to test player join notification for server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> TestAlertNotification(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/discord/test/alert", null);
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to test alert notification for server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== STATISTICS EXTENDED HANDLERS ====================

    private static async Task<IResult> GetPlayerStats(
        string serverId,
        string playerName,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/statistics/players/{Uri.EscapeDataString(playerName)}");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get player stats from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetMatchDetails(
        string serverId,
        long matchId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/statistics/matches/{matchId}");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get match details from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetAllServerStats(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/statistics/servers");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get all server stats from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== EVENTS EXTENDED HANDLERS ====================

    private static async Task<IResult> GetEventsByServer(
        string serverId,
        int instanceId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/events/server/{instanceId}");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get events by server from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetEventsByType(
        string serverId,
        string eventType,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/events/type/{Uri.EscapeDataString(eventType)}");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get events by type from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetMqttPublishableEvents(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/events/mqtt-publishable");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get MQTT publishable events from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== CHARTS EXTENDED HANDLERS ====================

    private static async Task<IResult> GetServerUptimeChart(
        string serverId,
        int instanceId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/charts/uptime/{instanceId}");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get server uptime chart from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetResourceCharts(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/charts/resources");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get resource charts from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> GetMatchSummary(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/charts/matches/summary");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to get match summary from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== ADVANCED METRICS EXTENDED HANDLERS ====================

    private static async Task<IResult> CompareServers(
        string serverId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            // Forward query string parameters
            var queryString = httpContext.Request.QueryString.Value ?? "";
            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/metrics/advanced/compare{queryString}");
            var content = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to compare servers from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== KICK PLAYER HANDLER ====================

    private static async Task<IResult> KickPlayer(
        string serverId,
        int instanceId,
        HttpContext httpContext,
        PortalDbContext db,
        [FromServices] IHttpClientFactory httpClientFactory,
        ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var requestBody = await new StreamReader(httpContext.Request.Body).ReadToEndAsync();
            var content = new StringContent(requestBody, System.Text.Encoding.UTF8, "application/json");
            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/servers/{instanceId}/kick", content);
            var responseContent = await response.Content.ReadAsStringAsync();
            var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
            return Results.Ok(data);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to kick player from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== FILEBEAT HANDLERS ====================

    private static async Task<IResult> GetFilebeatStatus(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/filebeat/status", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> InstallFilebeat(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, "/api/filebeat/install", null, httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> StartFilebeat(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, "/api/filebeat/start", null, httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> StopFilebeat(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, "/api/filebeat/stop", null, httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> ConfigureFilebeat(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequestWithBody(serverId, "/api/filebeat/configure", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> TestFilebeat(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, "/api/filebeat/test", null, httpContext, db, httpClientFactory, logger);

    // ==================== RBAC HANDLERS ====================

    private static async Task<IResult> GetRbacPermissions(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/rbac/permissions", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetRbacRoles(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/rbac/roles", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> CreateRbacRole(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequestWithBody(serverId, "/api/rbac/roles", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetRbacRole(string serverId, string roleName, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, $"/api/rbac/roles/{roleName}", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> UpdateRbacRole(string serverId, string roleName, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPutRequestWithBody(serverId, $"/api/rbac/roles/{roleName}", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> DeleteRbacRole(string serverId, string roleName, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyDeleteRequest(serverId, $"/api/rbac/roles/{roleName}", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> UpdateRbacRolePermissions(string serverId, string roleName, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPutRequestWithBody(serverId, $"/api/rbac/roles/{roleName}/permissions", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetUserRbacPermissions(string serverId, string userId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, $"/api/rbac/users/{userId}/permissions", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetUserRbacRoles(string serverId, string userId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, $"/api/rbac/users/{userId}/roles", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> UpdateUserRbacRoles(string serverId, string userId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPutRequestWithBody(serverId, $"/api/rbac/users/{userId}/roles", httpContext, db, httpClientFactory, logger);

    // ==================== DIAGNOSTICS HANDLERS ====================

    private static async Task<IResult> GetSkippedFrames(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/diagnostics/skipped-frames", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetSkippedFramesByServer(string serverId, int instanceId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, $"/api/diagnostics/skipped-frames/server/{instanceId}", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetSkippedFramesByPlayer(string serverId, string playerName, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, $"/api/diagnostics/skipped-frames/player/{Uri.EscapeDataString(playerName)}", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> ResetSkippedFrames(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, "/api/diagnostics/skipped-frames/reset", null, httpContext, db, httpClientFactory, logger);

    // ==================== STORAGE HANDLERS ====================

    private static async Task<IResult> GetStorageStatus(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/storage/status", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetStorageAnalytics(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/storage/analytics", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> RelocateStorage(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequestWithBody(serverId, "/api/storage/relocate", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> CleanupStorage(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequestWithBody(serverId, "/api/storage/cleanup", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> RelocateLogs(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequestWithBody(serverId, "/api/storage/relocate-logs", httpContext, db, httpClientFactory, logger);

    // ==================== GIT HANDLERS ====================

    private static async Task<IResult> GetCurrentBranch(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/git/branch", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetBranches(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/git/branches", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> SwitchBranch(string serverId, string branchName, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, $"/api/git/switch/{branchName}", null, httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetGitUpdates(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/git/updates", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> PullGitUpdates(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, "/api/git/pull", null, httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetGitVersion(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/git/version", httpContext, db, httpClientFactory, logger);

    // ==================== SERVER SCALING SERVICE HANDLERS ====================

    private static async Task<IResult> GetServerScalingStatus(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/server-scaling/status", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> AddScalingServers(string serverId, int count, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, $"/api/server-scaling/add/{count}", null, httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> RemoveScalingServers(string serverId, int count, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, $"/api/server-scaling/remove/{count}", null, httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> ScaleToServerCount(string serverId, int count, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, $"/api/server-scaling/scale-to/{count}", null, httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> AutoBalanceServers(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, "/api/server-scaling/auto-balance", null, httpContext, db, httpClientFactory, logger);

    // ==================== BACKUPS HANDLERS ====================

    private static async Task<IResult> GetBackups(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/backups", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> CreateBackup(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequestWithBody(serverId, "/api/backups", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetBackupDetails(string serverId, string backupId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, $"/api/backups/{backupId}", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> DeleteBackup(string serverId, string backupId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyDeleteRequest(serverId, $"/api/backups/{backupId}", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> RestoreBackup(string serverId, string backupId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, $"/api/backups/{backupId}/restore", null, httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> DownloadBackup(string serverId, string backupId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
    {
        // Special handling for download to stream the file
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(10); // Longer timeout for downloads

            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}/api/backups/{backupId}/download", HttpCompletionOption.ResponseHeadersRead);
            
            if (!response.IsSuccessStatusCode)
            {
                var content = await response.Content.ReadAsStringAsync();
                return Results.Json(new { error = $"Remote server returned {response.StatusCode}: {content}" }, statusCode: (int)response.StatusCode);
            }

            var stream = await response.Content.ReadAsStreamAsync();
            var contentType = response.Content.Headers.ContentType?.ToString() ?? "application/octet-stream";
            var fileName = response.Content.Headers.ContentDisposition?.FileName?.Trim('"') ?? $"backup-{backupId}.zip";

            return Results.File(stream, contentType, fileName);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to download backup from server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    // ==================== MQTT EXTENDED HANDLERS ====================

    private static async Task<IResult> PublishMqttMessage(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequestWithBody(serverId, "/api/mqtt/publish", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> PublishMqttTestMessage(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequest(serverId, "/api/mqtt/publish-test", null, httpContext, db, httpClientFactory, logger);

    // ==================== PERFORMANCE EXTENDED HANDLERS ====================

    private static async Task<IResult> GetPerformanceServers(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/performance/servers", httpContext, db, httpClientFactory, logger);

    // ==================== HEALTH EXTENDED HANDLERS ====================

    private static async Task<IResult> ValidateHealthIp(string serverId, string ipAddress, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, $"/api/health/ip/{ipAddress}", httpContext, db, httpClientFactory, logger);

    // ==================== CONSOLE COMMAND HANDLER ====================

    private static async Task<IResult> ExecuteConsoleCommand(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyPostRequestWithBody(serverId, "/api/console/execute", httpContext, db, httpClientFactory, logger);

    // ==================== SYSTEM STATISTICS HANDLER ====================

    private static async Task<IResult> GetSystemStats(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/system/stats", httpContext, db, httpClientFactory, logger);

    // ==================== VERSION INFO HANDLER ====================

    private static async Task<IResult> GetServerVersion(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/version", httpContext, db, httpClientFactory, logger);

    // ==================== REPLAY UPLOAD MANAGEMENT HANDLERS ====================

    private static async Task<IResult> UploadReplayFile(string serverId, string matchId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromMinutes(5);

            var requestContent = new StreamContent(httpContext.Request.Body);
            if (httpContext.Request.ContentType != null)
            {
                requestContent.Headers.TryAddWithoutValidation("Content-Type", httpContext.Request.ContentType);
            }

            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}/api/replays/upload/{matchId}", requestContent);
            var content = await response.Content.ReadAsStringAsync();
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = $"Remote server returned {response.StatusCode}: {content}" }, statusCode: (int)response.StatusCode);
            }

            try 
            {
                var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
                return Results.Ok(data);
            }
            catch
            {
                return Results.Ok(new { message = content });
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to upload replay to server {ServerId}", serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> DeleteUploadedReplay(string serverId, string fileName, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyDeleteRequest(serverId, $"/api/replays/upload/{fileName}", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetReplayUploadStats(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/replays/upload/stats", httpContext, db, httpClientFactory, logger);

    // ==================== PUBLIC INFO PROXIES HANDLERS ====================

    private static async Task<IResult> GetPublicServerInfo(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/public/get_server_info", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetPublicHonVersion(string serverId, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, "/api/public/get_hon_version", httpContext, db, httpClientFactory, logger);

    private static async Task<IResult> GetPublicSkippedFrameData(string serverId, int port, HttpContext httpContext, PortalDbContext db, [FromServices] IHttpClientFactory httpClientFactory, ILogger<Program> logger)
        => await ProxyGetRequest(serverId, $"/api/public/get_skipped_frame_data/{port}", httpContext, db, httpClientFactory, logger);

    // ==================== HELPER METHODS ====================

    private static async Task<IResult> ProxyGetRequest(string serverId, string endpoint, HttpContext httpContext, PortalDbContext db, IHttpClientFactory httpClientFactory, ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Viewer);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var queryString = httpContext.Request.QueryString.Value ?? "";
            var response = await client.GetAsync($"http://{server.IpAddress}:{server.ApiPort}{endpoint}{queryString}");
            var content = await response.Content.ReadAsStringAsync();
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = $"Remote server returned {response.StatusCode}: {content}" }, statusCode: (int)response.StatusCode);
            }

            try 
            {
                var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
                return Results.Ok(data);
            }
            catch
            {
                // If not JSON, return as string object
                return Results.Ok(new { message = content });
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to proxy GET request to {Endpoint} on server {ServerId}", endpoint, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ProxyPostRequest(string serverId, string endpoint, object? body, HttpContext httpContext, PortalDbContext db, IHttpClientFactory httpClientFactory, ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            HttpResponseMessage response;
            if (body != null)
            {
                response = await client.PostAsJsonAsync($"http://{server.IpAddress}:{server.ApiPort}{endpoint}", body);
            }
            else
            {
                response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}{endpoint}", null);
            }

            var content = await response.Content.ReadAsStringAsync();
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = $"Remote server returned {response.StatusCode}: {content}" }, statusCode: (int)response.StatusCode);
            }

            try 
            {
                var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
                return Results.Ok(data);
            }
            catch
            {
                return Results.Ok(new { message = content });
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to proxy POST request to {Endpoint} on server {ServerId}", endpoint, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ProxyPostRequestWithBody(string serverId, string endpoint, HttpContext httpContext, PortalDbContext db, IHttpClientFactory httpClientFactory, ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var requestBody = await new StreamReader(httpContext.Request.Body).ReadToEndAsync();
            var content = new StringContent(requestBody, System.Text.Encoding.UTF8, "application/json");
            
            var response = await client.PostAsync($"http://{server.IpAddress}:{server.ApiPort}{endpoint}", content);
            var responseContent = await response.Content.ReadAsStringAsync();
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = $"Remote server returned {response.StatusCode}: {responseContent}" }, statusCode: (int)response.StatusCode);
            }

            try 
            {
                var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
                return Results.Ok(data);
            }
            catch
            {
                return Results.Ok(new { message = responseContent });
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to proxy POST request to {Endpoint} on server {ServerId}", endpoint, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ProxyPutRequestWithBody(string serverId, string endpoint, HttpContext httpContext, PortalDbContext db, IHttpClientFactory httpClientFactory, ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var requestBody = await new StreamReader(httpContext.Request.Body).ReadToEndAsync();
            var content = new StringContent(requestBody, System.Text.Encoding.UTF8, "application/json");
            
            var response = await client.PutAsync($"http://{server.IpAddress}:{server.ApiPort}{endpoint}", content);
            var responseContent = await response.Content.ReadAsStringAsync();
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = $"Remote server returned {response.StatusCode}: {responseContent}" }, statusCode: (int)response.StatusCode);
            }

            try 
            {
                var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(responseContent);
                return Results.Ok(data);
            }
            catch
            {
                return Results.Ok(new { message = responseContent });
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to proxy PUT request to {Endpoint} on server {ServerId}", endpoint, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }

    private static async Task<IResult> ProxyDeleteRequest(string serverId, string endpoint, HttpContext httpContext, PortalDbContext db, IHttpClientFactory httpClientFactory, ILogger<Program> logger)
    {
        var user = await AuthEndpoints.GetAuthenticatedUser(httpContext, db);
        if (user == null) return Results.Unauthorized();

        var (server, role, error) = await GetServerWithAccess(db, user, serverId, ServerRole.Operator);
        if (error != null) return Results.Json(new { error });

        try
        {
            var client = httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("X-Api-Key", server!.ApiKey);
            client.Timeout = TimeSpan.FromSeconds(30);

            var response = await client.DeleteAsync($"http://{server.IpAddress}:{server.ApiPort}{endpoint}");
            var content = await response.Content.ReadAsStringAsync();
            
            if (!response.IsSuccessStatusCode)
            {
                return Results.Json(new { error = $"Remote server returned {response.StatusCode}: {content}" }, statusCode: (int)response.StatusCode);
            }

            try 
            {
                var data = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(content);
                return Results.Ok(data);
            }
            catch
            {
                return Results.Ok(new { message = content });
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to proxy DELETE request to {Endpoint} on server {ServerId}", endpoint, serverId);
            return Results.Json(new { error = ex.Message });
        }
    }
}

// Request DTOs
public record CliCommandRequest(string Command, string[]? Args);
