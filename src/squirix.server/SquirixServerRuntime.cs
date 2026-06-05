using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;

namespace Squirix.Server;

/// <summary>
/// Server-package bootstrap for the squirix node host runtime.
/// </summary>
internal static class SquirixServerRuntime
{
    /// <summary>
    /// Starts the squirix node server application with default production logging and cluster settings resolution.
    /// </summary>
    /// <param name="configure">Optional callback applied to server options before startup.</param>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A lifetime handle for the started application.</returns>
    public static async ValueTask<SquirixServerApplicationHandle> StartAsync(Action<SquirixServerOptions>? configure = null, CancellationToken cancellationToken = default)
    {
        var options = SquirixServerConfiguration.LoadOrCreateDefault();
        configure?.Invoke(options);
        SquirixServerConfiguration.ApplyRuntimeDefaults(options);
        ClusterTopologyValidator.Validate(options);

        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                Args = [],
                ApplicationName = typeof(SquirixServer).Assembly.GetName().Name,
            });

        _ = builder.Logging.ClearProviders();
        _ = builder.Logging.AddConsole();
        _ = builder.Logging.AddDebug();
        _ = builder.Logging.AddFilter("Grpc", LogLevel.Information);
        _ = builder.Logging.AddFilter("Grpc.AspNetCore.Server", LogLevel.Information);
        _ = builder.Logging.AddFilter("Squirix", LogLevel.Debug);

        _ = builder.AddSquirixServer(target => SquirixServerConfiguration.CopyOptions(options, target), loadDiscoveredSettings: false);
        var app = builder.Build();
        _ = app.MapSquirixServer();

        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        return new SquirixServerApplicationHandle(app);
    }
}
