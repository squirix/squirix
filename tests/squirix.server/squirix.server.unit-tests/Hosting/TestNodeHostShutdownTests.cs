using System;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies graceful node shutdown and its interplay with disposal.</summary>
public sealed class TestNodeHostShutdownTests
{
    /// <summary>Shutdown stops the node and stays safe to repeat and to follow with disposal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShutdownAsyncStopsNodeIdempotently(CancellationToken cancellationToken)
    {
        using var held = ListenPortPool.ServerUnitTests.HoldPort();
        var cluster = TestCluster<ClusterStartOptions>.Create(new ClusterNode("nodeA", held.HttpUri));
        var host = await cluster.StartNodeAsync("nodeA", cancellationToken: cancellationToken);
        try
        {
            // Two direct ShutdownAsync calls on the same host must both be no-throw and idempotent.
            await host.ShutdownAsync();
            await host.ShutdownAsync();

            AssertPortReleased(held.Port);

            // Disposing an already-shut-down host must also be a safe no-op.
            await host.DisposeAsync();

            AssertPortReleased(held.Port);
        }
        finally
        {
            // The host is still registered on the cluster (StopNodeAsync was never called), so this
            // exercises a third idempotent shutdown through the cluster's own teardown path.
            await cluster.DisposeAsync();
        }
    }

    /// <summary>An abrupt shutdown followed by a graceful shutdown attempt stays safe and idempotent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbruptThenShutdownIsIdempotent(CancellationToken cancellationToken)
    {
        using var held = ListenPortPool.ServerUnitTests.HoldPort();
        var cluster = TestCluster<ClusterStartOptions>.Create(new ClusterNode("nodeA", held.HttpUri));
        var host = await cluster.StartNodeAsync("nodeA", cancellationToken: cancellationToken);
        try
        {
            // Simulate an unclean process termination; a later graceful shutdown attempt must still be safe.
            await host.AbruptShutdownAsync();
            await host.ShutdownAsync();

            AssertPortReleased(held.Port);

            await host.DisposeAsync();

            AssertPortReleased(held.Port);
        }
        finally
        {
            // The host is still registered on the cluster (StopNodeAsync was never called), so this
            // exercises a further idempotent shutdown through the cluster's own teardown path.
            await cluster.DisposeAsync();
        }
    }

    /// <summary>
    /// An abrupt shutdown drops a call that is still running, as a killed process would, instead of answering it with the failure of a
    /// disposed service.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbruptShutdownDropsInFlightCalls(CancellationToken cancellationToken)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var builder = WebApplication.CreateSlimBuilder();
        _ = builder.WebHost.UseUrls("http://127.0.0.1:0");
        _ = builder.Services.AddSingleton<DisposalProbe>();
        var app = builder.Build();

        // The handler answers once its service is disposed, as a call that reaches a torn-down node would.
        app.Run(async context =>
        {
            var probe = context.RequestServices.GetRequiredService<DisposalProbe>();
            entered.SetResult();
            await probe.Disposed.WaitAsync(CancellationToken.None);
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        });
        await app.StartAsync(cancellationToken);
        Uri? address = null;
        foreach (var url in app.Urls)
            address ??= new Uri(url);

        using var client = new HttpClient();
        await using ITestNodeHost host = new TestNodeHost(app, address!, string.Empty);
        var call = client.GetAsync(address, cancellationToken);
        await entered.Task.WaitAsync(cancellationToken);

        await host.AbruptShutdownAsync();
        _ = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(call);
    }

    /// <summary>A failing app stop still disposes the app and the owned scope, and the stop failure surfaces to the caller.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShutdownDisposesAppWhenStopThrows(CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateSlimBuilder();
        _ = builder.WebHost.UseUrls("http://127.0.0.1:0");
        _ = builder.Services.AddSingleton<DisposalProbe>();
        _ = builder.Services.AddHostedService(static _ => new FailingStopService());
        var app = builder.Build();
        var appProbe = app.Services.GetRequiredService<DisposalProbe>();
        await app.StartAsync(cancellationToken);

        using var scope = new DisposalProbe();
        await using ITestNodeHost host = new TestNodeHost(app, new Uri("http://127.0.0.1"), string.Empty, scope: scope);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(host.ShutdownAsync());

        _ = await Assert.That(appProbe.IsDisposed).IsTrue().Because("The app must be disposed even when its stop step throws.");
        _ = await Assert.That(scope.IsDisposed).IsTrue().Because("The owned scope must be disposed even when the app stop step throws.");
    }

    private static void AssertPortReleased(int port)
    {
        // The bind must fail only while a socket still listens on the port. On BSD/macOS a lingering connection
        // (TIME_WAIT or an aborted peer) blocks a bind without address reuse, so reuse is enabled there. On Windows
        // address reuse would let a bind succeed next to a live listener, so the default exclusive behavior stays.
        using var listener = new TcpListener(IPAddress.Loopback, port);
        if (!OperatingSystem.IsWindows())
            listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);

        listener.Start();
        listener.Stop();
    }

    private sealed class DisposalProbe : IDisposable
    {
        private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Disposed => _disposed.Task;

        internal bool IsDisposed => _disposed.Task.IsCompleted;

        public void Dispose() => _ = _disposed.TrySetResult();
    }

    private sealed class FailingStopService : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.FromException(new InvalidOperationException("Simulated app stop failure."));
    }
}
