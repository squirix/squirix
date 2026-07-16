using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;

namespace Squirix.Server;

/// <summary>
/// Convenience entry point for starting and owning a Squirix server host in tests and samples.
/// Production deployments typically use <see cref="AspNetCoreExtensions.AddSquirixServerAsync" /> or the standalone host tool.
/// </summary>
public sealed class SquirixServer : IAsyncDisposable
{
    private const string ApplicationAssemblyName = "Squirix.Server";
    private readonly ApplicationHandle _handle;

    private SquirixServer(ApplicationHandle handle)
    {
        _handle = handle ?? throw new ArgumentNullException(nameof(handle));
    }

    /// <summary>Starts the Squirix server host runtime using discovered settings or ephemeral defaults.</summary>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A server host lifetime handle.</returns>
    public static ValueTask<SquirixServer> StartAsync(CancellationToken cancellationToken = default) => StartAsync(null, cancellationToken);

    /// <summary>Ends this server host handle and releases the owned server application.</summary>
    /// <returns>A task that completes when the server host is disposed.</returns>
    public ValueTask DisposeAsync() => _handle.DisposeAsync();

    /// <summary>Starts the Squirix server host runtime using discovered settings or ephemeral defaults.</summary>
    /// <param name="configure">Optional callback applied to server options before startup.</param>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A server host lifetime handle.</returns>
    private static async ValueTask<SquirixServer> StartAsync(Action<SquirixServerOptions>? configure, CancellationToken cancellationToken = default)
    {
        var handle = await BuildAppHandleAsync(configure, cancellationToken).ConfigureAwait(false);
        return new SquirixServer(handle);
    }

    /// <summary>Starts the squirix node server application with default production logging and cluster settings resolution.</summary>
    /// <param name="configure">Optional callback applied to server options before startup.</param>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A lifetime handle for the started application.</returns>
    private static async ValueTask<ApplicationHandle> BuildAppHandleAsync(
        Action<SquirixServerOptions>? configure = null,
        CancellationToken cancellationToken = default)
    {
        var options = await Configurator.LoadOrCreateDefaultAsync(cancellationToken).ConfigureAwait(false);
        configure?.Invoke(options);
        Configurator.ApplyRuntimeDefaults(options);
        SquirixServerOptionsValidator.Validate(options);

        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                Args = [],
                ApplicationName = ApplicationAssemblyName,
            });

        _ = builder.Logging.ClearProviders();
        _ = builder.Logging.AddConsole();
        _ = builder.Logging.AddDebug();
        _ = builder.Logging.AddFilter("Grpc", LogLevel.Information);
        _ = builder.Logging.AddFilter("Grpc.AspNetCore.Server", LogLevel.Information);
        _ = builder.Logging.AddFilter("Squirix", LogLevel.Debug);

        _ = await builder.AddSquirixServerAsync(
            target => Configurator.CopyOptions(options, target),
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        var app = builder.Build();
        _ = app.MapSquirixServer();

        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        return new ApplicationHandle(app);
    }

    private sealed class ApplicationHandle : IAsyncDisposable
    {
        private readonly WebApplication _app;

        internal ApplicationHandle(WebApplication app)
        {
            _app = app ?? throw new ArgumentNullException(nameof(app));
        }

        /// <summary>Ends the server application and releases the owned ASP.NET Core host.</summary>
        /// <returns>A task that completes when the application is disposed.</returns>
        public ValueTask DisposeAsync() => _app.DisposeAsync();
    }
}
