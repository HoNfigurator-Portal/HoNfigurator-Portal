using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;

namespace HoNfigurator.ManagementPortal.Models;

/// <summary>
/// User who logged in via Discord
/// </summary>
public class PortalUser
{
    [Key]
    public int Id { get; set; }
    
    /// <summary>
    /// Discord User ID
    /// </summary>
    public string DiscordId { get; set; } = string.Empty;
    
    /// <summary>
    /// Discord Username
    /// </summary>
    public string Username { get; set; } = string.Empty;
    
    /// <summary>
    /// Discord Discriminator (if any)
    /// </summary>
    public string? Discriminator { get; set; }
    
    /// <summary>
    /// Discord Avatar hash
    /// </summary>
    public string? AvatarHash { get; set; }
    
    /// <summary>
    /// Discord Email (if provided)
    /// </summary>
    public string? Email { get; set; }
    
    /// <summary>
    /// When user first logged in
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// Last login time
    /// </summary>
    public DateTime LastLoginAt { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// User's session token
    /// </summary>
    public string? SessionToken { get; set; }
    
    /// <summary>
    /// Session expiry
    /// </summary>
    public DateTime? SessionExpiresAt { get; set; }
    
    /// <summary>
    /// Is this user a SuperAdmin (can access all servers)
    /// </summary>
    public bool IsSuperAdmin { get; set; } = false;
    
    /// <summary>
    /// Servers owned by this user
    /// </summary>
    public List<RegisteredServer> Servers { get; set; } = new();
    
    /// <summary>
    /// Get avatar URL
    /// </summary>
    public string GetAvatarUrl()
    {
        if (string.IsNullOrEmpty(AvatarHash))
            return $"https://cdn.discordapp.com/embed/avatars/{(int.Parse(DiscordId) >> 22) % 6}.png";
        return $"https://cdn.discordapp.com/avatars/{DiscordId}/{AvatarHash}.png";
    }
}

/// <summary>
/// Registered server in the management portal
/// </summary>
public class RegisteredServer
{
    [Key]
    public int Id { get; set; }
    
    /// <summary>
    /// Owner user ID (foreign key)
    /// </summary>
    public int OwnerId { get; set; }
    
    /// <summary>
    /// Navigation property to owner
    /// </summary>
    public PortalUser? Owner { get; set; }
    
    /// <summary>
    /// Unique server identifier (auto-generated or provided)
    /// </summary>
    public string ServerId { get; set; } = string.Empty;
    
    /// <summary>
    /// Display name for the server
    /// </summary>
    public string ServerName { get; set; } = string.Empty;
    
    /// <summary>
    /// Server's URL (for callbacks/links)
    /// </summary>
    public string ServerUrl { get; set; } = string.Empty;
    
    /// <summary>
    /// Server's IP address
    /// </summary>
    public string IpAddress { get; set; } = string.Empty;
    
    /// <summary>
    /// Server's API port for callbacks
    /// </summary>
    public int ApiPort { get; set; } = 5050;
    
    /// <summary>
    /// Server's region/location
    /// </summary>
    public string Region { get; set; } = "Unknown";
    
    /// <summary>
    /// HoNfigurator version
    /// </summary>
    public string Version { get; set; } = "Unknown";
    
    /// <summary>
    /// API key for authentication (auto-generated)
    /// </summary>
    public string ApiKey { get; set; } = string.Empty;
    
    /// <summary>
    /// When the server was first registered
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// Last time we received a status update
    /// </summary>
    public DateTime LastSeenAt { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// Is the server currently online/reachable
    /// </summary>
    public bool IsOnline { get; set; }
    
    /// <summary>
    /// Current status data (JSON)
    /// </summary>
    public string? StatusJson { get; set; }
    
    /// <summary>
    /// Shared access list for this server
    /// </summary>
    public List<ServerAccess> SharedAccess { get; set; } = new();
}

/// <summary>
/// Access role for shared servers
/// </summary>
public enum ServerRole
{
    /// <summary>
    /// Can only view server status and instances
    /// </summary>
    Viewer = 0,
    
    /// <summary>
    /// Can view and control instances (start/stop/restart)
    /// </summary>
    Operator = 1,
    
    /// <summary>
    /// Can view, control, and modify configuration
    /// </summary>
    Admin = 2,
    
    /// <summary>
    /// Full owner access (can manage access, delete server)
    /// </summary>
    Owner = 3
}

/// <summary>
/// Shared access entry for a server
/// </summary>
public class ServerAccess
{
    [Key]
    public int Id { get; set; }
    
    /// <summary>
    /// Server ID (foreign key)
    /// </summary>
    public int ServerId { get; set; }
    
    /// <summary>
    /// Navigation property to server
    /// </summary>
    public RegisteredServer? Server { get; set; }
    
    /// <summary>
    /// User ID who has access (foreign key, nullable until user registers)
    /// </summary>
    public int? UserId { get; set; }
    
    /// <summary>
    /// Navigation property to user
    /// </summary>
    public PortalUser? User { get; set; }
    
    /// <summary>
    /// Discord ID of the user (for display before user logs in)
    /// </summary>
    public string DiscordId { get; set; } = string.Empty;
    
    /// <summary>
    /// Role/permission level
    /// </summary>
    public ServerRole Role { get; set; } = ServerRole.Viewer;
    
    /// <summary>
    /// When the access was granted
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    
    /// <summary>
    /// Who granted the access (user ID)
    /// </summary>
    public int GrantedById { get; set; }
}

/// <summary>
/// Request to add a new server host
/// </summary>
public class AddServerRequest
{
    [JsonPropertyName("server_name")]
    public string ServerName { get; set; } = string.Empty;
    
    [JsonPropertyName("ip_address")]
    public string IpAddress { get; set; } = string.Empty;
    
    [JsonPropertyName("api_port")]
    public int ApiPort { get; set; } = 5050;
    
    [JsonPropertyName("region")]
    public string Region { get; set; } = "Unknown";
}

/// <summary>
/// Request to update server host
/// </summary>
public class UpdateServerRequest
{
    [JsonPropertyName("server_name")]
    public string? ServerName { get; set; }
    
    [JsonPropertyName("ip_address")]
    public string? IpAddress { get; set; }
    
    [JsonPropertyName("api_port")]
    public int? ApiPort { get; set; }
    
    [JsonPropertyName("region")]
    public string? Region { get; set; }
}

/// <summary>
/// Server status report from HoNfigurator instance
/// </summary>
public class ServerStatusReport
{
    [JsonPropertyName("server_id")]
    public string ServerId { get; set; } = string.Empty;
    
    [JsonPropertyName("server_name")]
    public string ServerName { get; set; } = string.Empty;
    
    [JsonPropertyName("server_ip")]
    public string ServerIp { get; set; } = string.Empty;
    
    [JsonPropertyName("api_port")]
    public int ApiPort { get; set; }
    
    [JsonPropertyName("status")]
    public string Status { get; set; } = "Offline";
    
    [JsonPropertyName("version")]
    public string Version { get; set; } = "Unknown";
    
    [JsonPropertyName("hon_version")]
    public string? HonVersion { get; set; }
    
    [JsonPropertyName("honfigurator_version")]
    public string? HonfiguratorVersion { get; set; }
    
    [JsonPropertyName("timestamp")]
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    
    [JsonPropertyName("total_servers")]
    public int TotalServers { get; set; }
    
    [JsonPropertyName("online_servers")]
    public int OnlineServers { get; set; }
    
    [JsonPropertyName("total_players")]
    public int TotalPlayers { get; set; }
    
    [JsonPropertyName("master_server_connected")]
    public bool MasterServerConnected { get; set; }
    
    [JsonPropertyName("chat_server_connected")]
    public bool ChatServerConnected { get; set; }
    
    [JsonPropertyName("instances")]
    public List<ServerInstanceStatus>? Instances { get; set; }
    
    [JsonPropertyName("system_stats")]
    public SystemStatsReport? SystemStats { get; set; }
}

public class ServerInstanceStatus
{
    [JsonPropertyName("id")]
    public int Id { get; set; }
    
    [JsonPropertyName("name")]
    public string Name { get; set; } = string.Empty;
    
    [JsonPropertyName("port")]
    public int Port { get; set; }
    
    [JsonPropertyName("status")]
    public string Status { get; set; } = "Offline";
    
    [JsonPropertyName("num_clients")]
    public int NumClients { get; set; }
    
    [JsonPropertyName("match_id")]
    public long? MatchId { get; set; }
    
    [JsonPropertyName("game_phase")]
    public string? GamePhase { get; set; }
    
    [JsonPropertyName("map")]
    public string? Map { get; set; }
    
    [JsonPropertyName("game_mode")]
    public string? GameMode { get; set; }
    
    [JsonPropertyName("start_time")]
    public DateTime? StartTime { get; set; }
}

public class SystemStatsReport
{
    [JsonPropertyName("cpu_percent")]
    public double CpuPercent { get; set; }
    
    [JsonPropertyName("cpu_count")]
    public int CpuCount { get; set; }
    
    [JsonPropertyName("memory_percent")]
    public double MemoryPercent { get; set; }
    
    [JsonPropertyName("total_memory_mb")]
    public long TotalMemoryMb { get; set; }
    
    [JsonPropertyName("used_memory_mb")]
    public long UsedMemoryMb { get; set; }
    
    [JsonPropertyName("uptimeSeconds")]
    public long UptimeSeconds { get; set; }
}

/// <summary>
/// Registration request from HoNfigurator
/// </summary>
public class RegistrationRequest
{
    [JsonPropertyName("server_id")]
    public string ServerId { get; set; } = string.Empty;
    
    [JsonPropertyName("server_name")]
    public string ServerName { get; set; } = string.Empty;
    
    [JsonPropertyName("server_url")]
    public string ServerUrl { get; set; } = string.Empty;
    
    [JsonPropertyName("ip_address")]
    public string IpAddress { get; set; } = string.Empty;
    
    [JsonPropertyName("api_port")]
    public int ApiPort { get; set; }
    
    [JsonPropertyName("region")]
    public string Region { get; set; } = "Unknown";
    
    [JsonPropertyName("version")]
    public string Version { get; set; } = "Unknown";
    
    [JsonPropertyName("discord_user_id")]
    public string? DiscordUserId { get; set; }
}

/// <summary>
/// Registration response to HoNfigurator
/// </summary>
public class RegistrationResponse
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }
    
    [JsonPropertyName("server_id")]
    public string? ServerId { get; set; }
    
    [JsonPropertyName("api_key")]
    public string? ApiKey { get; set; }
    
    [JsonPropertyName("message")]
    public string Message { get; set; } = string.Empty;
    
    [JsonPropertyName("server_name")]
    public string? ServerName { get; set; }
    
    [JsonPropertyName("server_address")]
    public string? ServerAddress { get; set; }
}

/// <summary>
/// Dashboard summary response
/// </summary>
public class DashboardSummary
{
    public int TotalServers { get; set; }
    public int OnlineServers { get; set; }
    public int TotalGameServers { get; set; }
    public int ActiveGameServers { get; set; }
    public int TotalPlayers { get; set; }
    public int ActiveMatches { get; set; }
    public List<RegisteredServerSummary> Servers { get; set; } = new();
}

public class RegisteredServerSummary
{
    public string ServerId { get; set; } = string.Empty;
    public string ServerName { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public bool IsOnline { get; set; }
    public DateTime LastSeenAt { get; set; }
    public int TotalServers { get; set; }
    public int OnlineServers { get; set; }
    public int TotalPlayers { get; set; }
    public double CpuPercent { get; set; }
    public double MemoryPercent { get; set; }
}

/// <summary>
/// Request to scale instances
/// </summary>
public class ScaleRequest
{
    [JsonPropertyName("targetCount")]
    public int TargetCount { get; set; }
}

/// <summary>
/// Request to broadcast message
/// </summary>
public class BroadcastRequest
{
    [JsonPropertyName("message")]
    public string Message { get; set; } = string.Empty;
}

/// <summary>
/// Request to update server configuration
/// </summary>
public class ConfigUpdateRequest
{
    [JsonPropertyName("proxyEnabled")]
    public bool? ProxyEnabled { get; set; }
    
    [JsonPropertyName("basePort")]
    public int? BasePort { get; set; }
    
    [JsonPropertyName("maxPlayers")]
    public int? MaxPlayers { get; set; }
    
    [JsonPropertyName("totalServers")]
    public int? TotalServers { get; set; }
}

/// <summary>
/// Request for auto-registration from HoNfigurator
/// </summary>
public class AutoRegisterRequest
{
    [JsonPropertyName("discord_user_id")]
    public string? DiscordUserId { get; set; }
    
    [JsonPropertyName("discord_username")]
    public string? DiscordUsername { get; set; }
    
    [JsonPropertyName("server_name")]
    public string? ServerName { get; set; }
    
    [JsonPropertyName("ip_address")]
    public string? IpAddress { get; set; }
    
    [JsonPropertyName("api_port")]
    public int? ApiPort { get; set; }
    
    [JsonPropertyName("region")]
    public string? Region { get; set; }
    
    [JsonPropertyName("version")]
    public string? Version { get; set; }
}

/// <summary>
/// Response for auto-registration
/// </summary>
public class AutoRegisterResponse
{
    [JsonPropertyName("success")]
    public bool Success { get; set; }
    
    [JsonPropertyName("message")]
    public string Message { get; set; } = string.Empty;
    
    [JsonPropertyName("server_id")]
    public string? ServerId { get; set; }
    
    [JsonPropertyName("api_key")]
    public string? ApiKey { get; set; }
    
    [JsonPropertyName("is_new_registration")]
    public bool IsNewRegistration { get; set; }
}

/// <summary>
/// Request to add access for a user to a server
/// </summary>
public class AddAccessRequest
{
    [JsonPropertyName("discord_id")]
    public string DiscordId { get; set; } = string.Empty;
    
    [JsonPropertyName("role")]
    public ServerRole Role { get; set; } = ServerRole.Viewer;
}

/// <summary>
/// Request to update access role
/// </summary>
public class UpdateAccessRequest
{
    [JsonPropertyName("role")]
    public ServerRole Role { get; set; }
}

/// <summary>
/// Response for access entry
/// </summary>
public class AccessEntryResponse
{
    [JsonPropertyName("id")]
    public int Id { get; set; }
    
    [JsonPropertyName("discord_id")]
    public string DiscordId { get; set; } = string.Empty;
    
    [JsonPropertyName("username")]
    public string? Username { get; set; }
    
    [JsonPropertyName("avatar_url")]
    public string? AvatarUrl { get; set; }
    
    [JsonPropertyName("role")]
    public ServerRole Role { get; set; }
    
    [JsonPropertyName("role_name")]
    public string RoleName => Role.ToString();
    
    [JsonPropertyName("created_at")]
    public DateTime CreatedAt { get; set; }
    
    [JsonPropertyName("is_registered")]
    public bool IsRegistered { get; set; }
}

/// <summary>
/// Request to set SuperAdmin status
/// </summary>
public class SetSuperAdminRequest
{
    [JsonPropertyName("is_super_admin")]
    public bool IsSuperAdmin { get; set; }
}
