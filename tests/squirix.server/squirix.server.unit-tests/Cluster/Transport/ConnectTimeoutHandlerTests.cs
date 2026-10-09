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

/// <summary>A connect that timed out is reported as a connect failure; every other cancellation passes through unchanged.</summary>
public sealed class ConnectTimeoutHandlerTests
{
    private static readonly Uri Peer = new("https://localhost:6500/");

    /// <summary>A connect that outlasts the connect timeout fails the request with a connection error caused by the timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectTimeoutIsConnectionError(CancellationToken cancellationToken)
    {
        var failure = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(SendThroughAsync(null, TimeSpan.FromMilliseconds(50), null, cancellationToken));

        _ = await Assert.That(failure.HttpRequestError).IsEqualTo(HttpRequestError.ConnectionError);
        _ = await Assert.That(failure.InnerException).IsTypeOf<IOException>();
        _ = await Assert.That(failure.InnerException?.InnerException?.InnerException).IsTypeOf<TimeoutException>();
    }

    /// <summary>A dial that outlasts the dial timeout fails the request with a connection error caused by an I/O failure carrying the timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DialTimeoutIsConnectionError(CancellationToken cancellationToken)
    {
        var failure = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(SendThroughAsync(null, TimeSpan.FromMinutes(1), TimeSpan.FromMilliseconds(50), cancellationToken));

        _ = await Assert.That(failure.HttpRequestError).IsEqualTo(HttpRequestError.ConnectionError);
        _ = await Assert.That(failure.InnerException).IsTypeOf<IOException>();
        _ = await Assert.That(failure.InnerException?.InnerException).IsTypeOf<TimeoutException>();
    }

    /// <summary>A request its caller cancels while it connects stays a cancellation, as a per-attempt timeout or a deadline would.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallerCancelStaysCancellation(CancellationToken cancellationToken)
    {
        var dialing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var send = SendThroughAsync(dialing, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1), caller.Token);
        await dialing.Task.WaitAsync(cancellationToken);

        await caller.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(send);
    }

    /// <summary>The pool's timeout bounds a handler it owns and a factory handler left infinite; a factory handler keeps a timeout it chose.</summary>
    [Test]
    public async Task FactoryKeepsItsOwnTimeout()
    {
        var bound = TimeSpan.FromMilliseconds(300);
        var chosen = TimeSpan.FromSeconds(1);
        using var owned = new SocketsHttpHandler { ConnectTimeout = chosen };
        using var factoryChosen = new SocketsHttpHandler { ConnectTimeout = chosen };
        using var factoryInfinite = new SocketsHttpHandler();
        using var factoryWrapped = new SocketsHttpHandler();
        using var outer = new PassThroughHandler(factoryWrapped);

        using var ownedWrapper = ConnectTimeoutHandler.Wrap(owned, bound, true, null);
        using var chosenWrapper = ConnectTimeoutHandler.Wrap(factoryChosen, bound, false, null);
        using var infiniteWrapper = ConnectTimeoutHandler.Wrap(factoryInfinite, bound, false, null);
        using var wrappedWrapper = ConnectTimeoutHandler.Wrap(outer, bound, false, null);

        _ = await Assert.That(owned.ConnectTimeout).IsEqualTo(bound);
        _ = await Assert.That(factoryChosen.ConnectTimeout).IsEqualTo(chosen);
        _ = await Assert.That(factoryInfinite.ConnectTimeout).IsEqualTo(bound);
        _ = await Assert.That(factoryWrapped.ConnectTimeout).IsEqualTo(bound);
    }

    /// <summary>A connect timeout that is not positive is refused.</summary>
    /// <param name="ticks">The timeout in ticks.</param>
    [Test]
    [Arguments(0L)]
    [Arguments(-TimeSpan.TicksPerMillisecond)]
    public void NonPositiveTimeoutIsRefused(long ticks)
    {
        using var handler = new SocketsHttpHandler();

        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws((handler, ticks), static state => _ = ConnectTimeoutHandler.Wrap(state.handler, TimeSpan.FromTicks(state.ticks), true, null));
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

    /// <summary>Sends one request through a wrapped sockets handler whose connects never complete.</summary>
    /// <param name="dialing">Completed once the connect started; <see langword="null" /> when not observed.</param>
    /// <param name="connectTimeout">The connect timeout of the handler.</param>
    /// <param name="dialTimeout">The dial timeout of the handler; <see langword="null" /> for none.</param>
    /// <param name="cancellationToken">The request token.</param>
    /// <returns>A task that fails as the request does.</returns>
    private static async Task SendThroughAsync(TaskCompletionSource? dialing, TimeSpan connectTimeout, TimeSpan? dialTimeout, CancellationToken cancellationToken)
    {
        using var sockets = new SocketsHttpHandler { ConnectCallback = (_, ct) => NeverConnectsAsync(dialing, ct) };
        using var invoker = new HttpMessageInvoker(ConnectTimeoutHandler.Wrap(sockets, connectTimeout, true, dialTimeout));
        using var request = new HttpRequestMessage(HttpMethod.Get, Peer);
        using var response = await invoker.SendAsync(request, cancellationToken);
    }

    /// <summary>A delegating handler that only forwards, standing for a factory's own wrapper.</summary>
    private sealed class PassThroughHandler : DelegatingHandler
    {
        internal PassThroughHandler(HttpMessageHandler inner)
            : base(inner)
        {
        }
    }
}
