using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies gRPC detailed error exposure follows the host environment.</summary>
public sealed class GrpcDetailedErrorsHostingTests : ServerUnitTestBase
{
    /// <summary>Ensures development hosts keep detailed gRPC diagnostics available intentionally.</summary>
    [Fact]
    public async Task DevelopmentHostEnablesDetailedGrpcErrorsByDefault()
    {
        await using var app = await BuildHostAsync("Development", DefaultCancellationToken);
        var options = app.Services.GetRequiredService<IOptions<GrpcServiceOptions>>().Value;
        Assert.True(options.EnableDetailedErrors);
    }

    /// <summary>Ensures production-like hosts do not enable detailed gRPC errors by default.</summary>
    [Fact]
    public async Task ProductionHostDisablesDetailedGrpcErrorsByDefault()
    {
        await using var app = await BuildHostAsync("Production", DefaultCancellationToken);
        var options = app.Services.GetRequiredService<IOptions<GrpcServiceOptions>>().Value;
        Assert.False(options.EnableDetailedErrors);
    }

    private static async Task<WebApplication> BuildHostAsync(string environmentName, CancellationToken cancellationToken)
    {
        var applicationOptions = new WebApplicationOptions
        {
            EnvironmentName = environmentName,
        };
        var builder = WebApplication.CreateBuilder(applicationOptions);

        _ = await builder.AddSquirixServerAsync(
            static options => options.Uri = new Uri(InvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort())),
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);

        return builder.Build();
    }
}
