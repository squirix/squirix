using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server;

/// <summary>Starts a native Squirix server and owns its lifetime.</summary>
[Immutable]
public sealed class SquirixServer : IAsyncDisposable
{
    private readonly ApplicationHandle _handle;

    private SquirixServer(ApplicationHandle handle)
    {
        ArgumentNullException.ThrowIfNull(handle);
        _handle = handle;
    }

    /// <summary>Gets the bound HTTPS address of the management listener.</summary>
    public Uri ManagementUri => _handle.ManagementUri;

    /// <summary>Gets the signal raised when the host receives a shutdown request.</summary>
    public CancellationToken ApplicationStopping => _handle.ApplicationStopping;

    /// <summary>Starts the Squirix server host runtime using discovered settings or ephemeral defaults.</summary>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A server host lifetime handle.</returns>
    /// <exception cref="PlatformNotSupportedException">Thrown on Linux or macOS when the .NET 10 runtime is older than 10.0.15.</exception>
    public static ValueTask<SquirixServer> StartAsync(CancellationToken cancellationToken = default) => StartAsync(configure: null, cancellationToken);

    /// <summary>Starts a node with an explicitly configured topology and persistence settings.</summary>
    /// <param name="options">Node settings for this instance.</param>
    /// <param name="cancellationToken">Cancels startup.</param>
    /// <returns>The running node.</returns>
    /// <exception cref="PlatformNotSupportedException">The runtime does not provide the required flush guarantees.</exception>
    public static async ValueTask<SquirixServer> StartAsync(SquirixServerOptions options, CancellationToken cancellationToken = default)
    {
        if ((OperatingSystem.IsLinux() || OperatingSystem.IsMacOS()) && Environment.Version is { Major: 10, Build: < 15 })
            throw new PlatformNotSupportedException($"Squirix.Server requires .NET 10.0.15 or later on Linux and macOS to report disk flush failures. The current runtime is {Environment.Version}.");
        Configurator.ApplyRuntimeDefaults(options);
        options.Validate();

        var services = new NodeServices();
        var environment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production";
        var configuration = new ServerLoggingSettings(Path.Join(AppContext.BaseDirectory, "appsettings.defaults.json"), Directory.GetCurrentDirectory(), environment);
        try
        {
#pragma warning disable RS0030 // The standalone composition root supplies the console writer to its injected logging factory.
            _ = services.Own<ILoggerFactory>(_ => new ServerLoggerFactory(configuration, Console.Out.WriteLine));
#pragma warning restore RS0030
            await NodeConfiguration.ConfigureNodeAsync(services, options, null, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            configuration.Dispose();
            throw;
        }

#pragma warning disable CA2000 // StartApplicationAsync takes ownership, including rollback on startup failure.
        var application = ServerApplication.Build(services, configuration);
#pragma warning restore CA2000
        var handle = await ApplicationHandle.StartApplicationAsync(application, static (app, token) => ServerHostingComposition.OpenStorageAsync(app.Services, token), cancellationToken).ConfigureAwait(false);
        return new SquirixServer(handle);
    }

    /// <summary>
    /// Ends this server host handle and releases the owned server application. The host is stopped gracefully within the host shutdown timeout,
    /// stop failures are logged rather than thrown, and then the host is released.
    /// </summary>
    /// <returns>A task that completes when the server host is disposed.</returns>
    public ValueTask DisposeAsync() => _handle.DisposeAsync();

    /// <summary>Starts the Squirix server host runtime using discovered settings or ephemeral defaults.</summary>
    /// <param name="configure">Optional callback applied to server options before startup.</param>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A server host lifetime handle.</returns>
    internal static async ValueTask<SquirixServer> StartAsync(Action<SquirixServerOptions>? configure, CancellationToken cancellationToken = default)
    {
        var options = await Configurator.LoadOrCreateDefaultAsync(cancellationToken).ConfigureAwait(false);
        configure?.Invoke(options);
        return await StartAsync(options, cancellationToken).ConfigureAwait(false);
    }

    [ThreadSafe]
    internal sealed class ApplicationHandle : IAsyncDisposable
    {
        private readonly ServerApplication _app;
        private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _disposeStarted;

        internal ApplicationHandle(ServerApplication app)
        {
            ArgumentNullException.ThrowIfNull(app);
            _app = app;
        }

        internal Uri ManagementUri => _app.Services.Require<NativeClientService>().ManagementUri!;

        internal CancellationToken ApplicationStopping => _app.Services.Require<INodeLifetime>().ApplicationStopping;

        /// <summary>Stops the server application, then releases the owned host; repeated and concurrent calls share one disposal.</summary>
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
        internal static async ValueTask<ApplicationHandle> StartApplicationAsync(ServerApplication app, Func<ServerApplication, CancellationToken, Task> configure, CancellationToken cancellationToken)
        {
            await StartWithRollbackAsync(app, configure, cancellationToken).ConfigureAwait(false);
            return new ApplicationHandle(app);
        }

        internal static async Task StartWithRollbackAsync(ServerApplication app, Func<ServerApplication, CancellationToken, Task> configure, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(app);
            ArgumentNullException.ThrowIfNull(configure);
            var logger = app.Services.Require<ILoggerFactory>().CreateLogger<SquirixServer>();
#pragma warning disable CA1031 // The original startup failure is rethrown; cleanup and its logging must never replace it.
            try
            {
                await configure(app, cancellationToken).ConfigureAwait(false);
                await app.StartAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                try
                {
                    await new ApplicationHandle(app).DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception cleanupException)
                {
                    LogCleanupFailure(logger, cleanupException);
                }

                throw;
            }
#pragma warning restore CA1031
        }

        private static void LogCleanupFailure(ILogger logger, Exception cleanupException)
        {
            // A failing logger must not replace the startup failure that is being rethrown, and a logging
            // failure has nowhere else to go without global state, so it is discarded by design.
            _ = Isolated.Run(
                (Logger: logger, Cleanup: cleanupException),
                static state => DiagnosticsLog.HostDisposeFailedAfterStartFailure(state.Logger, state.Cleanup));
        }

        private async Task RunDisposeAsync()
        {
#pragma warning disable CA1031 // A failed stop is logged and disposal continues; any other failure is delivered to every caller through the shared task.
            try
            {
                var logger = _app.Services.Require<ILoggerFactory>().CreateLogger<SquirixServer>();
                try
                {
                    // Journal draining has its own durability budget after the other services have stopped.
                    await _app.StopAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    DiagnosticsLog.HostStopFailedOnDispose(logger, ex);
                }
                finally
                {
                    await _app.DisposeAsync().ConfigureAwait(false);
                }

                _disposed.SetResult();
            }
            catch (Exception ex)
            {
                _disposed.SetException(ex);
            }
#pragma warning restore CA1031
        }
    }
}
