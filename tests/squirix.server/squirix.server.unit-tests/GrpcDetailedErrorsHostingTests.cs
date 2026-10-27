using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies gRPC detailed error exposure follows the host environment.</summary>
[Immutable]
public sealed class GrpcDetailedErrorsHostingTests : ServerUnitTestBase
{
    /// <summary>Ensures development hosts keep detailed gRPC diagnostics available intentionally.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DevelopmentHostEnablesDetailedErrors(CancellationToken cancellationToken)
    {
        await using var app = await BuildHostAsync("Development", cancellationToken);
        var options = app.Services.GetRequiredService<IOptions<GrpcServiceOptions>>().Value;
        _ = await Assert.That(options.EnableDetailedErrors).IsTrue();
    }

    /// <summary>Ensures production-like hosts do not enable detailed gRPC errors by default.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionHostDisablesDetailedErrors(CancellationToken cancellationToken)
    {
        await using var app = await BuildHostAsync("Production", cancellationToken);
        var options = app.Services.GetRequiredService<IOptions<GrpcServiceOptions>>().Value;
        _ = await Assert.That(options.EnableDetailedErrors).IsFalse();
    }

    private static async Task<WebApplication> BuildHostAsync(string environmentName, CancellationToken cancellationToken)
    {
        var applicationOptions = new WebApplicationOptions
        {
            EnvironmentName = environmentName,
        };
        var builder = WebApplication.CreateBuilder(applicationOptions);

        _ = await builder.AddSquirixServerAsync(
            static options => options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort())),
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);

        return builder.Build();
    }
}
