using System;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.TestKit.Http;
using Xunit;

namespace Squirix.Server.UnitTests.Security;

/// <summary>
/// Verifies gRPC detailed error exposure follows the host environment.
/// </summary>
public sealed class GrpcDetailedErrorsHostingTests
{
    /// <summary>
    /// Ensures production-like hosts do not enable detailed gRPC errors by default.
    /// </summary>
    [Fact]
    public void ProductionHostDisablesDetailedGrpcErrorsByDefault()
    {
        using var app = BuildHost("Production");
        var options = app.Services.GetRequiredService<IOptions<GrpcServiceOptions>>().Value;
        Assert.False(options.EnableDetailedErrors);
    }

    /// <summary>
    /// Ensures development hosts keep detailed gRPC diagnostics available intentionally.
    /// </summary>
    [Fact]
    public void DevelopmentHostEnablesDetailedGrpcErrorsByDefault()
    {
        using var app = BuildHost("Development");
        var options = app.Services.GetRequiredService<IOptions<GrpcServiceOptions>>().Value;
        Assert.True(options.EnableDetailedErrors);
    }

    private static WebApplication BuildHost(string environmentName)
    {
        var port = new PortAllocator(30000, 30999).Allocate();
        var applicationOptions = new WebApplicationOptions
        {
            EnvironmentName = environmentName,
        };
        var builder = WebApplication.CreateBuilder(applicationOptions);

        _ = builder.AddSquirixServer(
            options => options.Url = new Uri($"https://localhost:{port}"),
            loadDiscoveredSettings: false);

        return builder.Build();
    }
}
