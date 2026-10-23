using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>A dial that timed out is reported as a connect failure; a cancellation of the request passes through unchanged.</summary>
public sealed class BoundedDialTests
{
    private static readonly Uri Peer = new("https://localhost:6500/");

    /// <summary>A dial that outlasts the dial timeout fails the request with a connection error caused by an I/O failure carrying the timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DialTimeoutIsConnectionError(CancellationToken cancellationToken)
    {
        var failure = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(SendThroughAsync(null, TimeSpan.FromMilliseconds(50), cancellationToken));

        _ = await Assert.That(failure.HttpRequestError).IsEqualTo(HttpRequestError.ConnectionError);
        _ = await Assert.That(failure.InnerException).IsTypeOf<IOException>();
        _ = await Assert.That(failure.InnerException?.InnerException).IsTypeOf<TimeoutException>();
    }

    /// <summary>Within one dial timeout after a dial timed out, the next request fails the same way at once, without dialing again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimedOutDialFailsNextAtOnce(CancellationToken cancellationToken)
    {
        var dials = new int[1];
        using var sockets = new SocketsHttpHandler { ConnectCallback = (_, ct) => CountedNeverConnectsAsync(dials, ct) };
        BoundedDial.Apply(sockets, TimeSpan.FromSeconds(1));
        using var invoker = new HttpMessageInvoker(sockets, false);

        _ = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(SendAsync(invoker, cancellationToken));
        var second = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(SendAsync(invoker, cancellationToken));

        _ = await Assert.That(second.HttpRequestError).IsEqualTo(HttpRequestError.ConnectionError);
        _ = await Assert.That(second.InnerException?.InnerException).IsTypeOf<TimeoutException>();
        _ = await Assert.That(Volatile.Read(ref dials[0])).IsEqualTo(1);
    }

    /// <summary>A request its caller cancels while it dials stays a cancellation, as a per-attempt timeout or a deadline would.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallerCancelStaysCancellation(CancellationToken cancellationToken)
    {
        var dialing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var send = SendThroughAsync(dialing, TimeSpan.FromMinutes(1), caller.Token);
        await dialing.Task.WaitAsync(cancellationToken);

        await caller.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(send);
    }

    /// <summary>A dial bound below the minimum is refused.</summary>
    /// <param name="ticks">The bound in ticks.</param>
    [Test]
    [Arguments(0L)]
    [Arguments(TimeSpan.TicksPerMillisecond * 9)]
    public void ShortDialTimeoutIsRefused(long ticks)
    {
        using var handler = new SocketsHttpHandler { ConnectCallback = static (_, ct) => NeverConnectsAsync(null, ct) };

        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws((handler, ticks), static state => BoundedDial.Apply(state.handler, TimeSpan.FromTicks(state.ticks)));
    }

    /// <summary>A handler without the connect callback that tracks its connections is refused rather than dialed untracked.</summary>
    [Test]
    public void MissingCallbackIsRefused()
    {
        using var handler = new SocketsHttpHandler();

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(handler, static h => BoundedDial.Apply(h, TimeSpan.FromSeconds(1)));
    }

    /// <summary>Counts a dial and waits for a connect that never completes until it is canceled.</summary>
    /// <param name="dials">The dial count, in its only element.</param>
    /// <param name="cancellationToken">The connect token.</param>
    /// <returns>A task that only ever fails with the cancellation.</returns>
    private static ValueTask<Stream> CountedNeverConnectsAsync(int[] dials, CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref dials[0]);
        return NeverConnectsAsync(null, cancellationToken);
    }

    /// <summary>Waits for a connect that never completes until it is canceled.</summary>
    /// <param name="dialing">Completed once the connect started; <see langword="null" /> when not observed.</param>
    /// <param name="cancellationToken">The connect token.</param>
    /// <returns>A task that only ever fails with the cancellation.</returns>
    private static async ValueTask<Stream> NeverConnectsAsync(TaskCompletionSource? dialing, CancellationToken cancellationToken)
    {
        _ = dialing?.TrySetResult();
        var never = new TaskCompletionSource<Stream>(TaskCreationOptions.RunContinuationsAsynchronously);
        return await never.Task.WaitAsync(cancellationToken);
    }

    /// <summary>Sends one request through <paramref name="invoker" />.</summary>
    /// <param name="invoker">The invoker.</param>
    /// <param name="cancellationToken">The request token.</param>
    /// <returns>A task that fails as the request does.</returns>
    private static async Task SendAsync(HttpMessageInvoker invoker, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, Peer);
        using var response = await invoker.SendAsync(request, cancellationToken);
    }

    /// <summary>Sends one request through a sockets handler whose dials never complete, bounded by <paramref name="dialTimeout" />.</summary>
    /// <param name="dialing">Completed once the dial started; <see langword="null" /> when not observed.</param>
    /// <param name="dialTimeout">The dial timeout.</param>
    /// <param name="cancellationToken">The request token.</param>
    /// <returns>A task that fails as the request does.</returns>
    private static async Task SendThroughAsync(TaskCompletionSource? dialing, TimeSpan dialTimeout, CancellationToken cancellationToken)
    {
        using var sockets = new SocketsHttpHandler { ConnectCallback = (_, ct) => NeverConnectsAsync(dialing, ct) };
        BoundedDial.Apply(sockets, dialTimeout);
        using var invoker = new HttpMessageInvoker(sockets, false);
        await SendAsync(invoker, cancellationToken);
    }
}
