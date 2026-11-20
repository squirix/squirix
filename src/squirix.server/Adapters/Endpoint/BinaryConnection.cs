using System;
using System.Globalization;
using System.IO.Pipelines;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Peer addresses, authenticated identity and shutdown signals of a public binary session.</summary>
internal sealed class BinaryConnection : IDisposable
{
    private static long _sequence;
    private readonly CancellationTokenSource _closed;
    private readonly CancellationTokenSource _draining;

    internal BinaryConnection(IDuplexPipe transport, IPEndPoint? local, IPEndPoint? remote, CancellationToken graceful, CancellationToken abort)
    {
        Transport = transport;
        LocalEndPoint = local;
        RemoteEndPoint = remote;
        ConnectionId = Interlocked.Increment(ref _sequence).ToString("x", CultureInfo.InvariantCulture);
        _closed = CancellationTokenSource.CreateLinkedTokenSource(abort);
        _draining = CancellationTokenSource.CreateLinkedTokenSource(graceful);
    }

    internal IDuplexPipe Transport { get; }

    internal string ConnectionId { get; }

    internal IPEndPoint? LocalEndPoint { get; }

    internal IPEndPoint? RemoteEndPoint { get; }

    internal CancellationToken ConnectionClosed => _closed.Token;

    internal CancellationToken CloseRequested => _draining.Token;

    internal AuthenticatedConnection? Identity { get; set; }

    public void Dispose()
    {
        _draining.Dispose();
        _closed.Dispose();
    }

    internal async ValueTask CompleteTransportAsync()
    {
        try
        {
            await Transport.Input.CompleteAsync().ConfigureAwait(false);
        }
        finally
        {
            // StreamPipeWriter flushes retained bytes on normal completion. An aborted session must discard them.
            var error = _closed.IsCancellationRequested ? new OperationCanceledException(ConnectionClosed) : null;
            await Transport.Output.CompleteAsync(error).ConfigureAwait(false);
        }
    }

    // The pump observes cancellation before returning from its synchronous abort callback.
#pragma warning disable MA0045
    internal void Abort() => _closed.Cancel();

    internal void RequestClose() => _draining.Cancel();
#pragma warning restore MA0045
}
