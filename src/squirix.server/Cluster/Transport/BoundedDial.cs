using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Bounds the dial of a connection, the part before any TLS handshake, and reports a dial that timed out as a failed connect.</summary>
/// <remarks>
/// <para>
/// A host that is down or drops connection attempts never completes the dial, while a live host completes it at once even when it is too
/// loaded to finish a TLS handshake quickly. Bounding only the dial therefore fails the connect to a dead host fast without aborting the slow
/// but healthy handshakes of a loaded one, which the handler's own connect timeout keeps bounding.
/// </para>
/// <para>
/// A dial that timed out returned no stream, so nothing was sent. It fails with an <see cref="IOException" /> that carries a
/// <see cref="TimeoutException" />; the sockets handler reports it as an <see cref="HttpRequestException" /> with
/// <see cref="HttpRequestError.ConnectionError" />, which a gRPC client reports as unavailable with that cause. A cancellation of the connect
/// itself passes through unchanged.
/// </para>
/// <para>
/// The sockets handler keeps one pending HTTP/2 connection per endpoint, so requests that wait for it would each pay a full dial bound in
/// turn. For one dial bound after a dial timed out, every new dial of the handler fails at once the same way instead of dialing, so all of
/// them fail promptly; a dial that connects ends that window. One instance serves one handler, which dials one peer.
/// </para>
/// </remarks>
[ThreadSafe]
internal sealed class BoundedDial
{
    private const string TimedOut = "The connection to the peer was not established within the dial timeout; nothing was sent.";

    private const string TimedOutRecently = "A dial to the peer timed out less than one dial timeout ago, so this one was not attempted; nothing was sent.";

    private readonly Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>> _inner;
    private readonly TimeSpan _timeout;

    /// <summary>The timestamp of the last dial that timed out; zero when none did or a later dial connected.</summary>
    private long _timedOutAt;

    private BoundedDial(Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>> inner, TimeSpan timeout)
    {
        _inner = inner;
        _timeout = timeout;
    }

    /// <summary>Gets the shortest dial bound accepted.</summary>
    internal static TimeSpan MinTimeout { get; } = TimeSpan.FromMilliseconds(10);

    /// <summary>Bounds the dial of <paramref name="handler" /> by <paramref name="timeout" />, keeping its connect callback as the dial.</summary>
    /// <param name="handler">A handler that has not started.</param>
    /// <param name="timeout">The dial timeout.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout" /> is below <see cref="MinTimeout" />.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="handler" /> has no connect callback: the pool dials, and tracks, every connection through one.</exception>
    internal static void Apply(SocketsHttpHandler handler, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, MinTimeout);
        var inner = ThrowHelper.Required(handler.ConnectCallback, "A bounded dial needs the connect callback that tracks the connection; the handler has none.");
        handler.ConnectCallback = new BoundedDial(inner, timeout).ConnectAsync;
    }

    private async ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken cancellationToken)
    {
        var timedOutAt = Interlocked.Read(ref _timedOutAt);
        if (timedOutAt != 0 && TimeProvider.System.GetElapsedTime(timedOutAt) < _timeout)
            throw new IOException(TimedOutRecently, new TimeoutException(TimedOutRecently));

        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bounded.CancelAfter(_timeout);
        try
        {
            var stream = await _inner(context, bounded.Token).ConfigureAwait(false);
            _ = Interlocked.Exchange(ref _timedOutAt, 0);
            return stream;
        }
        catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested && bounded.IsCancellationRequested)
        {
            _ = Interlocked.Exchange(ref _timedOutAt, TimeProvider.System.GetTimestamp());
            throw new IOException(TimedOut, new TimeoutException(TimedOut, exception));
        }
    }
}
