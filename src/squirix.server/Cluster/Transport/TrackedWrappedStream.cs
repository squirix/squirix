using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

/// <summary>A connection that a handler outside the pool's ownership dialed through its own connect callback, tracked in the pool's connection gate until the stream is disposed.</summary>
/// <remarks>The wrapper only forwards; the wrapped stream stays the transport, so a redirecting connect callback keeps working unchanged.</remarks>
[Mutable]
internal sealed class TrackedWrappedStream : Stream
{
    private readonly TrackedConnections _connections;

    private readonly Stream _inner;

    private int _released;

    private TrackedWrappedStream(Stream inner, TrackedConnections connections)
    {
        _inner = inner;
        _connections = connections;
    }

    /// <inheritdoc />
    public override bool CanRead => _inner.CanRead;

    /// <inheritdoc />
    public override bool CanSeek => false;

    /// <inheritdoc />
    public override bool CanWrite => _inner.CanWrite;

    /// <inheritdoc />
    public override long Length => throw new NotSupportedException();

    /// <inheritdoc />
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    /// <inheritdoc />
    public override void Flush() => _inner.Flush();

    /// <inheritdoc />
    public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

    /// <inheritdoc />
    public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

    /// <inheritdoc />
    public override int Read(Span<byte> buffer) => _inner.Read(buffer);

    /// <inheritdoc />
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => _inner.ReadAsync(buffer, cancellationToken);

    /// <inheritdoc />
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.ReadAsync(buffer, offset, count, cancellationToken);

    /// <inheritdoc />
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    /// <inheritdoc />
    public override void SetLength(long value) => throw new NotSupportedException();

    /// <inheritdoc />
    public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);

    /// <inheritdoc />
    public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);

    /// <inheritdoc />
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) => _inner.WriteAsync(buffer, cancellationToken);

    /// <inheritdoc />
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => _inner.WriteAsync(buffer, offset, count, cancellationToken);

    /// <summary>Connects through <paramref name="inner" /> and tracks the resulting connection in <paramref name="connections" />.</summary>
    /// <param name="connections">The pool's connections.</param>
    /// <param name="context">The connection request.</param>
    /// <param name="inner">The connect callback the handler had before the pool took over.</param>
    /// <param name="cancellationToken">Cancels the connect attempt.</param>
    /// <returns>The tracked stream, which owns the wrapped stream.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="connections" />, <paramref name="context" /> or <paramref name="inner" /> is null.</exception>
    /// <exception cref="ObjectDisposedException">The pool started to dispose and admits no new connection.</exception>
    internal static async ValueTask<Stream> ConnectAsync(
        TrackedConnections connections,
        SocketsHttpConnectionContext context,
        Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>> inner,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connections);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(inner);
        if (!connections.TryEnter())
            throw new ObjectDisposedException(nameof(ServerClientPool), "The server client pool is disposing and admits no new connection.");

        TrackedWrappedStream? tracked = null;
        Stream? connected = null;
        try
        {
            connected = await inner.Invoke(context, cancellationToken).ConfigureAwait(false);
            tracked = new TrackedWrappedStream(connected, connections);
            connections.Register(tracked);
            return tracked;
        }
        catch
        {
            // Whatever failed, no stream leaves the gate later, so the entry is left here.
            if (tracked != null)
            {
                await tracked.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                if (connected != null)
                    await connected.DisposeAsync().ConfigureAwait(false);

                connections.Exit();
            }

            throw;
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        try
        {
            if (disposing)
                _inner.Dispose();
        }
        finally
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
            {
                _connections.Unregister(this);
                _connections.Exit();
            }

            base.Dispose(disposing);
        }
    }
}
