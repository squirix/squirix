using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Node.Hosting;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Stops the journal in the stopped phase of the host stop, after the stop phase of every hosted service (including the web server that
/// drains in-flight requests), so their last appends are accepted, drained, and made durable before the journal refuses new work. The stopped
/// phase of lifecycle services registered earlier runs after this one, so they must not append; with concurrent stops the stopped phases
/// run concurrently, so a lifecycle service that must append does so in its stop phase.
/// </summary>
internal sealed class JournalStopService : IHostedLifecycleService
{
    private readonly JournalCoordinatorHost _host;

    internal JournalStopService(JournalCoordinatorHost host)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StoppedAsync(CancellationToken cancellationToken) =>

        // The host token is ignored on purpose: the journal's own budget and stage floors bound the stop, and giving up early would only
        // move the drain into disposal. A failure propagates so the host reports it.
        _host.StopAsync().AsTask();

    /// <inheritdoc />
    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
