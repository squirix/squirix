using System;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Connection tracking of the owned internode handlers: a connect that fails before a stream exists leaves no entry behind.</summary>
[Immutable]
public sealed class TrackedConnectionStreamTests : ServerUnitTestBase
{
    /// <summary>A refused connect leaves the connection count at zero.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedConnectLeavesNoConnection(CancellationToken cancellationToken)
    {
        var connections = new TrackedConnections();
        using var handler = CreateHandler(connections);
        using var invoker = new HttpMessageInvoker(handler, false);
        using var message = new HttpRequestMessage(HttpMethod.Get, new Uri($"https://127.0.0.1:{ReserveClosedPort()}/"));

        _ = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(invoker.SendAsync(message, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(connections.Pending).IsEqualTo(0);
    }

    /// <summary>A connect after the connections were closed is refused with an object-disposed failure and is not counted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectAfterCloseIsRefused(CancellationToken cancellationToken)
    {
        var connections = new TrackedConnections();
        connections.Close();
        using var handler = CreateHandler(connections);
        using var invoker = new HttpMessageInvoker(handler, false);
        using var message = new HttpRequestMessage(HttpMethod.Get, new Uri($"https://127.0.0.1:{ReserveClosedPort()}/"));

        var failure = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(invoker.SendAsync(message, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(failure.InnerException).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(connections.Pending).IsEqualTo(0);
    }

    /// <summary>
    /// A stream whose base constructor threw is still finalized with its fields unset; disposing it must neither throw nor touch connections
    /// it never tracked.
    /// </summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task HalfBuiltStreamDisposesHarmlessly()
    {
        var stream = Unsafe.As<TrackedConnectionStream>(RuntimeHelpers.GetUninitializedObject(typeof(TrackedConnectionStream)));

        await stream.DisposeAsync();

        _ = await Assert.That(stream.CanRead).IsFalse();
    }

    /// <summary>Independent calls can abandon a blocked lookup and a later dial resolves the peer again after its address changes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DialCancellationPreservesSharedLookup(CancellationToken cancellationToken)
    {
        var first = new TaskCompletionSource<IPAddress[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var lookups = 0;
        var addresses = new PeerHostAddresses(new DnsEndPoint("peer", 5001), _ =>
            ++lookups == 1 ? first.Task : Task.FromResult(new[] { IPAddress.IPv6Loopback }));
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var cancelled = addresses.ResolveAsync(deadline.Token);
        var survivor = addresses.ResolveAsync(cancellationToken);

        await deadline.CancelAsync();
        _ = await Assert.That(cancelled.IsCanceled).IsTrue();
        _ = await Assert.That(survivor.IsCompleted).IsFalse();
        _ = await Assert.That(lookups).IsEqualTo(1);

        first.SetResult([IPAddress.Loopback]);
        _ = await Assert.That((await survivor)[0]).IsEqualTo(IPAddress.Loopback);
        _ = await Assert.That((await addresses.ResolveAsync(cancellationToken))[0]).IsEqualTo(IPAddress.IPv6Loopback);
        _ = await Assert.That(lookups).IsEqualTo(2);
    }

    /// <summary>A failed DNS lookup is retried when the next call arrives after the peer returns.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NextDialRetriesFailedLookup(CancellationToken cancellationToken)
    {
        var first = new TaskCompletionSource<IPAddress[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var lookups = 0;
        var addresses = new PeerHostAddresses(new DnsEndPoint("peer", 5001), _ =>
            ++lookups == 1 ? first.Task : Task.FromResult(new[] { IPAddress.Loopback }));
        var pending = addresses.ResolveAsync(cancellationToken);
        first.SetException(new SocketException());

        _ = await NodeAsyncAssert.ThrowsAsync<SocketException>(pending, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        _ = await Assert.That((await addresses.ResolveAsync(cancellationToken))[0]).IsEqualTo(IPAddress.Loopback);
        _ = await Assert.That(lookups).IsEqualTo(2);
    }

    private static SocketsHttpHandler CreateHandler(TrackedConnections connections) => new()
    {
        ConnectCallback = (context, cancellationToken) => TrackedConnectionStream.ConnectAsync(connections, context.DnsEndPoint, cancellationToken),
    };

    private static int ReserveClosedPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return listener.LocalEndpoint is IPEndPoint endpoint ? endpoint.Port : throw new InvalidOperationException("The listener bound no TCP port.");
    }
}
