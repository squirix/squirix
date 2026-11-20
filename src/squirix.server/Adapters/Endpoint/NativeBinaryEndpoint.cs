using System;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Runs the cache protocol on TLS sessions admitted by the node's public listener.</summary>
internal sealed class NativeBinaryEndpoint
{
    private readonly ConnectionDelegate _dispatch;
    private readonly IPEndPoint _local;

    internal NativeBinaryEndpoint(IServiceProvider services, IPEndPoint local, bool mutationReplay)
    {
        _local = local;
        _dispatch = BinaryEndpoint.Create(RejectAsync, services, mutationReplay);
    }

    internal static NativeTlsListener CreateListener(Socket socket, SslServerAuthenticationOptions tls, IServiceProvider services, bool mutationReplay, int capacity)
    {
        if (socket.LocalEndPoint is not IPEndPoint local)
            throw new InvalidOperationException("The public TLS endpoint must be bound before accepting clients.");
        var endpoint = new NativeBinaryEndpoint(services, local, mutationReplay);
        return new NativeTlsListener(socket, tls, endpoint.ServeAsync, services.GetRequiredService<ILogger<NativeTlsListener>>(), capacity);
    }

    private static Task RejectAsync(ConnectionContext connection)
    {
        connection.Abort();
        return Task.CompletedTask;
    }

    private async Task ServeAsync(SslStream stream, IPEndPoint remote, CancellationToken graceful, CancellationToken abort)
    {
        using var disconnected = CancellationTokenSource.CreateLinkedTokenSource(abort);
        using var draining = CancellationTokenSource.CreateLinkedTokenSource(graceful);
        var pipe = new StreamPipe(stream);
        var connection = new ClientConnection(disconnected, draining)
        {
            Transport = pipe,
            LocalEndPoint = _local,
            RemoteEndPoint = remote,
            ConnectionClosed = disconnected.Token,
        };
        await using var lifetime = connection.ConfigureAwait(false);
        try
        {
            await _dispatch(connection).ConfigureAwait(false);
        }
        finally
        {
            await pipe.Input.CompleteAsync().ConfigureAwait(false);
            await pipe.Output.CompleteAsync().ConfigureAwait(false);
        }
    }

    private sealed class StreamPipe : IDuplexPipe
    {
        internal StreamPipe(SslStream stream)
        {
            Input = PipeReader.Create(stream, new StreamPipeReaderOptions(leaveOpen: true));
            Output = PipeWriter.Create(stream, new StreamPipeWriterOptions(leaveOpen: true));
        }

        public PipeReader Input { get; }

        public PipeWriter Output { get; }
    }

    private sealed class ClientConnection : DefaultConnectionContext, IConnectionLifetimeNotificationFeature
    {
        private readonly CancellationTokenSource _disconnected;
        private readonly CancellationTokenSource _draining;

        internal ClientConnection(CancellationTokenSource disconnected, CancellationTokenSource draining)
        {
            _disconnected = disconnected;
            _draining = draining;
            ConnectionClosedRequested = draining.Token;
            Features.Set<IConnectionLifetimeNotificationFeature>(this);
        }

        public CancellationToken ConnectionClosedRequested { get; set; }

        public void RequestClose() => _draining.Cancel();

        public override void Abort(ConnectionAbortedException abortReason) => _disconnected.Cancel();
    }
}
