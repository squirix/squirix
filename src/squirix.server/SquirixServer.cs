using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server;

/// <summary>
/// Convenience entry point for starting and owning a Squirix server host in tests and samples.
/// Production deployments typically use <see cref="SquirixServerAspNetCoreExtensions.AddSquirixServer" /> or the standalone host tool.
/// </summary>
public sealed class SquirixServer : IAsyncDisposable
{
    private readonly SquirixServerApplicationHandle _handle;

    private SquirixServer(SquirixServerApplicationHandle handle)
    {
        _handle = handle ?? throw new ArgumentNullException(nameof(handle));
    }

    /// <summary>
    /// Starts the Squirix server host runtime using discovered settings or ephemeral defaults.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A server host lifetime handle.</returns>
    public static ValueTask<SquirixServer> StartAsync(CancellationToken cancellationToken = default) => StartAsync(null, cancellationToken);

    /// <summary>
    /// Ends this server host handle and releases the owned server application.
    /// </summary>
    /// <returns>A task that completes when the server host is disposed.</returns>
    public ValueTask DisposeAsync() => _handle.DisposeAsync();

    /// <summary>
    /// Starts the Squirix server host runtime using discovered settings or ephemeral defaults.
    /// </summary>
    /// <param name="configure">Optional callback applied to server options before startup.</param>
    /// <param name="cancellationToken">Cancellation token for server startup.</param>
    /// <returns>A server host lifetime handle.</returns>
    private static async ValueTask<SquirixServer> StartAsync(Action<SquirixServerOptions>? configure, CancellationToken cancellationToken = default)
    {
        var handle = await SquirixServerRuntime.StartAsync(configure, cancellationToken).ConfigureAwait(false);
        return new SquirixServer(handle);
    }
}
