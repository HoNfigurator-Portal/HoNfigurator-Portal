using Microsoft.EntityFrameworkCore;
using HoNfigurator.ManagementPortal.Data;
using HoNfigurator.ManagementPortal.Services;
using HoNfigurator.ManagementPortal.Hubs;
using HoNfigurator.ManagementPortal.Endpoints;

var builder = WebApplication.CreateBuilder(args);

// Get configuration
var portalConfig = builder.Configuration.GetSection("Portal");
var port = portalConfig.GetValue<int>("Port", 5200);
var bindAll = portalConfig.GetValue<bool>("BindToAllInterfaces", false);
var baseUrl = portalConfig.GetValue<string>("BaseUrl") ?? $"http://localhost:{port}";

// Configure Kestrel
builder.WebHost.ConfigureKestrel(serverOptions =>
{
    if (bindAll)
    {
        // Bind to all interfaces (0.0.0.0) - accessible from network
        serverOptions.ListenAnyIP(port);
    }
    else
    {
        // Bind to localhost only
        serverOptions.ListenLocalhost(port);
    }
});

// Add services
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() { Title = "HoNfigurator Management Portal API", Version = "v1" });
});

// Database
builder.Services.AddDbContext<PortalDbContext>(options =>
    options.UseSqlite(builder.Configuration.GetConnectionString("DefaultConnection") 
        ?? "Data Source=portal.db"));

// Services
builder.Services.AddSingleton<ServerStatusService>();
builder.Services.AddHostedService<ServerCleanupService>();
builder.Services.AddHttpClient(); // For proxying requests to game servers

// SignalR for real-time updates
builder.Services.AddSignalR();

// CORS for dashboard
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.AllowAnyOrigin()
              .AllowAnyMethod()
              .AllowAnyHeader();
    });
});

var app = builder.Build();

// Ensure database is created
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<PortalDbContext>();
    db.Database.EnsureCreated();
    
    // Ensure ServerAccess table exists (manual migration for existing databases)
    try
    {
        await db.Database.ExecuteSqlRawAsync(@"
            CREATE TABLE IF NOT EXISTS ServerAccess (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                ServerId INTEGER NOT NULL,
                UserId INTEGER,
                DiscordId TEXT NOT NULL,
                Role INTEGER NOT NULL DEFAULT 0,
                CreatedAt TEXT NOT NULL,
                GrantedById INTEGER NOT NULL,
                FOREIGN KEY (ServerId) REFERENCES Servers(Id) ON DELETE CASCADE
            )");
        
        await db.Database.ExecuteSqlRawAsync(@"
            CREATE UNIQUE INDEX IF NOT EXISTS IX_ServerAccess_ServerId_DiscordId 
            ON ServerAccess (ServerId, DiscordId)");
        
        await db.Database.ExecuteSqlRawAsync(@"
            CREATE INDEX IF NOT EXISTS IX_ServerAccess_DiscordId 
            ON ServerAccess (DiscordId)");
        
        await db.Database.ExecuteSqlRawAsync(@"
            CREATE INDEX IF NOT EXISTS IX_ServerAccess_UserId 
            ON ServerAccess (UserId)");
    }
    catch (Exception ex) { Console.WriteLine($"Note: Table creation: {ex.Message}"); }

    /*
    try
    {
        // Add IsSuperAdmin column to Users table if not exists
        await db.Database.ExecuteSqlRawAsync(@"
            ALTER TABLE Users ADD COLUMN IsSuperAdmin INTEGER NOT NULL DEFAULT 0");
    }
    catch (Exception ex)
    {
        // Ignore duplicate column error
        if (!ex.Message.Contains("duplicate column name"))
        {
            Console.WriteLine($"Note: Column migration: {ex.Message}");
        }
    }
    */
}

// Configure pipeline
app.UseSwagger();
app.UseSwaggerUI();

app.UseCors();
app.UseDefaultFiles();
app.UseStaticFiles();

// Map API endpoints
app.MapAuthEndpoints();
app.MapPortalEndpoints();

// Map SignalR hub
app.MapHub<PortalHub>("/hub/portal");

// Fallback to index.html for SPA
app.MapFallbackToFile("index.html");

Console.WriteLine("╔════════════════════════════════════════════════════════════╗");
Console.WriteLine("║     HoNfigurator Management Portal - Self Hosted           ║");
Console.WriteLine("╠════════════════════════════════════════════════════════════╣");
Console.WriteLine($"║  Portal URL: {baseUrl,-45} ║");
Console.WriteLine($"║  API Docs:   {baseUrl}/swagger{new string(' ', 45 - baseUrl.Length - 8)} ║");
Console.WriteLine($"║  Bind Mode:  {(bindAll ? "All Interfaces (0.0.0.0)" : "Localhost only"),-45} ║");
Console.WriteLine("╚════════════════════════════════════════════════════════════╝");

app.Run();
