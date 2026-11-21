using System;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>A dial that timed out is reported as a connect failure; a cancellation of the request passes through unchanged.</summary>
public sealed class BoundedDialTests
{
    /// <summary>A request its caller cancels while it dials stays a cancellation, as a per-attempt timeout or a deadline would.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallerCancelStaysCancellation(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        var dialing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await using var pool = peer.CreatePool(
            TimeSpan.FromMinutes(1),
            ct =>
            {
                _ = dialing.TrySetResult();
                return NeverConnectAsync(ct);
            });
        var send = NativePeerFixture.PingAsync(pool.ForNode(NativePeerFixture.Node), caller.Token);
        await dialing.Task.WaitAsync(cancellationToken);
        await caller.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(send, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
    }

    /// <summary>Both default and redirected peer endpoints register their live transport.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DefaultAndRedirectedEndpointsAreTracked(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        var resolutions = 0;
        await using var direct = peer.CreatePool();

        await using var redirected = peer.CreatePool(resolve: ResolveAsync, target: new Uri("https://127.0.0.1:1"));
        foreach (var pool in new[] { direct, redirected })
        {
            var ping = NativePeerFixture.PingAsync(pool.ForNode(NativePeerFixture.Node), cancellationToken);
            await using var stream = await peer.AcceptTlsAsync(cancellationToken);
            _ = await NativePeerFixture.ReadAsync(stream, cancellationToken);
            _ = await Assert.That(pool.OpenConnections).IsEqualTo(1);
            await NativePeerFixture.ReplyAsync(stream, cancellationToken);
            _ = await ping;
        }

        _ = await Assert.That(resolutions).IsEqualTo(1);
        return;

        ValueTask<EndPoint> ResolveAsync(CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            resolutions++;
            return ValueTask.FromResult<EndPoint>(peer.Endpoint);
        }
    }

    /// <summary>A dial that outlasts the dial timeout fails the request with a connection error caused by an I/O failure carrying the timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DialTimeoutIsConnectionError(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool(TimeSpan.FromMilliseconds(50), static ct => NeverConnectAsync(ct));
        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            NativePeerFixture.PingAsync(pool.ForNode(NativePeerFixture.Node), cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);
        await AssertDialTimeoutAsync(failure);
    }

    /// <summary>A dial bound below the minimum is refused.</summary>
    /// <param name="ticks">The bound in ticks.</param>
    [Test]
    [Arguments(0L)]
    [Arguments(TimeSpan.TicksPerMillisecond * 9)]
    public async Task ShortDialTimeoutIsRefused(long ticks) => _ = await Assert.That(ForwardingCallPolicyDefaults.ValidateConnectTimeout(TimeSpan.FromTicks(ticks))).IsNotNull();

    /// <summary>Within one dial timeout after a dial timed out, the next request fails the same way at once, without dialing again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimedOutDialFailsNextAtOnce(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        var dials = 0;
        await using var pool = peer.CreatePool(
            TimeSpan.FromSeconds(1),
            ct =>
            {
                _ = Interlocked.Increment(ref dials);
                return NeverConnectAsync(ct);
            });
        var client = pool.ForNode(NativePeerFixture.Node);
        for (var i = 0; i < 2; i++)
        {
            var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
                NativePeerFixture.PingAsync(client, cancellationToken),
                TimeSpan.FromSeconds(30),
                TimeProvider.System,
                cancellationToken);
            await AssertDialTimeoutAsync(failure);
        }

        _ = await Assert.That(dials).IsEqualTo(1);
    }

    private static async Task AssertDialTimeoutAsync(ServerCallException failure)
    {
        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        var connection = failure.InnerException as HttpRequestException;
        _ = await Assert.That(connection?.HttpRequestError).IsEqualTo(HttpRequestError.ConnectionError);
        _ = await Assert.That((connection?.InnerException as SocketException)?.SocketErrorCode).IsEqualTo(SocketError.TimedOut);
    }

    private static async ValueTask<EndPoint> NeverConnectAsync(CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<EndPoint>(TaskCreationOptions.RunContinuationsAsynchronously);
        return await completion.Task.WaitAsync(cancellationToken);
    }
}
