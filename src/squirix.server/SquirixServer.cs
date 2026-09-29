using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server;

/// <summary>
/// Convenience entry point for starting and owning a Squirix server host in tests and samples.
/// Production deployments typically use <see cref="AspNetCoreExtensions.AddSquirixServerAsync" /> or the standalone host tool.
/// </summary>
[Immutable]
public sealed class SquirixServer : IAsyncDisposable
{
    private const string ApplicationAssemblyName = "Squirix.Server";
    private readonly ApplicationHandle _handle;

    private SquirixServer(ApplicationHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);
        _handle = handle;
    }

    /// <summary>Starts the Squirix server host runtime using discovered settings or ephemeral defaults.</summary>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A server host lifetime handle.</returns>
    public static ValueTask<SquirixServer> StartAsync(CancellationToken cancellationToken = default) => StartAsync(null, cancellationToken);

    /// <summary>
    /// Ends this server host handle and releases the owned server application. The host is stopped gracefully within the host shutdown timeout,
    /// stop failures are logged rather than thrown, and then the host is released.
    /// </summary>
    /// <returns>A task that completes when the server host is disposed.</returns>
    public ValueTask DisposeAsync() => _handle.DisposeAsync();

    /// <summary>Starts the squirix node server application with default production logging and cluster settings resolution.</summary>
    /// <param name="configure">Optional callback applied to server options before startup.</param>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A lifetime handle for the started application.</returns>
    private static async ValueTask<ApplicationHandle> BuildAppHandleAsync(Action<SquirixServerOptions>? configure = null, CancellationToken cancellationToken = default)
    {
        var options = await Configurator.LoadOrCreateDefaultAsync(cancellationToken).ConfigureAwait(false);
        configure?.Invoke(options);
        Configurator.ApplyRuntimeDefaults(options);
        options.Validate();

        var applicationOptions = new WebApplicationOptions
        {
            Args = [],
            ApplicationName = ApplicationAssemblyName,
        };
        var builder = WebApplication.CreateBuilder(applicationOptions);
        _ = builder.Logging.ClearProviders();
        _ = builder.Logging.AddConsole();
        _ = builder.Logging.AddDebug();
        _ = builder.Logging.AddFilter("Grpc", LogLevel.Information);
        _ = builder.Logging.AddFilter("Grpc.AspNetCore.Server", LogLevel.Information);
        _ = builder.Logging.AddFilter("Squirix", LogLevel.Debug);

        _ = await builder.AddSquirixServerAsync(target => Configurator.CopyOptions(options, target), loadDiscoveredSettings: false, cancellationToken: cancellationToken)
                         .ConfigureAwait(false);
        var app = builder.Build();
        return await ApplicationHandle.StartApplicationAsync(app, static application => application.MapSquirixServer(), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Starts the Squirix server host runtime using discovered settings or ephemeral defaults.</summary>
    /// <param name="configure">Optional callback applied to server options before startup.</param>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A server host lifetime handle.</returns>
    private static async ValueTask<SquirixServer> StartAsync(Action<SquirixServerOptions>? configure, CancellationToken cancellationToken = default)
    {
        var handle = await BuildAppHandleAsync(configure, cancellationToken).ConfigureAwait(false);
        return new SquirixServer(handle);
    }

    [ThreadSafe]
    internal sealed class ApplicationHandle : IAsyncDisposable
    {
        private readonly WebApplication _app;
        private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _disposeStarted;

        internal ApplicationHandle(WebApplication app)
        {
            ArgumentNullException.ThrowIfNull(app);
            _app = app;
        }

        /// <summary>Stops the server application, then releases the owned ASP.NET Core host; repeated and concurrent calls share one disposal.</summary>
        /// <returns>A task that completes when the application is disposed.</returns>
        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposeStarted, 1) == 0)
                _ = RunDisposeAsync();

            return new ValueTask(_disposed.Task);
        }

        /// <summary>
        /// Configures and starts a built application. When configuration or startup fails, whatever already started is stopped and the application is
        /// released, then the original exception is rethrown; a cleanup failure is logged and never replaces it.
        /// </summary>
        /// <param name="app">The built application; ownership transfers to the returned handle or is released on failure.</param>
        /// <param name="configure">Callback applied to the built application before it starts.</param>
        /// <param name="cancellationToken">Cancellation token for startup only; cleanup does not observe it.</param>
        /// <returns>A handle owning the started application.</returns>
        internal static async ValueTask<ApplicationHandle> StartApplicationAsync(WebApplication app, Action<WebApplication> configure, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(app);
            ArgumentNullException.ThrowIfNull(configure);
            var handle = new ApplicationHandle(app);
            var logger = app.Logger;
            try
            {
                configure(app);
                await app.StartAsync(cancellationToken).ConfigureAwait(false);
                return handle;
            }
#pragma warning disable CA1031 // The original startup failure is rethrown; a cleanup failure is logged instead of replacing it.
            catch
#pragma warning restore CA1031
            {
                try
                {
                    await handle.DisposeAsync().ConfigureAwait(false);
                }
#pragma warning disable CA1031 // See above.
                catch (Exception cleanupException)
#pragma warning restore CA1031
                {
                    LogManager.HostDisposeFailedAfterStartFailure(logger, cleanupException);
                }

                throw;
            }
        }

        private async Task RunDisposeAsync()
        {
            try
            {
                var logger = _app.Logger;
                try
                {
                    // Host.StopAsync cancels this token after HostOptions.ShutdownTimeout; the hosted services honour it.
                    // It is a cancellation request rather than a hard deadline, so no extra bound is added here.
                    await _app.StopAsync(CancellationToken.None).ConfigureAwait(false);
                }
#pragma warning disable CA1031 // Disposal must not throw: a failed stop is logged and the host is still disposed.
                catch (Exception ex)
#pragma warning restore CA1031
                {
                    LogManager.HostStopFailedOnDispose(logger, ex);
                }
                finally
                {
                    await _app.DisposeAsync().ConfigureAwait(false);
                }

                _disposed.SetResult();
            }
#pragma warning disable CA1031 // The failure is delivered to every caller through the shared task.
            catch (Exception ex)
#pragma warning restore CA1031
            {
                _disposed.SetException(ex);
            }
        }
    }
}
