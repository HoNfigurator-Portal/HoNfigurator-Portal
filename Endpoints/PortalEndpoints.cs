using HoNfigurator.ManagementPortal.Data;
using HoNfigurator.ManagementPortal.Models;
using HoNfigurator.ManagementPortal.Services;
using HoNfigurator.ManagementPortal.Hubs;
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
                { Content = JsonContent.Create(new { serverIds = new[] { instanceId } }) });

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
}
