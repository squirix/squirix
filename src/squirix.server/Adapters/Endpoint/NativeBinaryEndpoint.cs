using System;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Runs the cache protocol on TLS sessions admitted by the node's public listener.</summary>
internal sealed class NativeBinaryEndpoint
{
    private readonly Func<BinaryConnection, Task> _dispatch;
    private readonly IPEndPoint _local;

    internal NativeBinaryEndpoint(IServiceProvider services, IPEndPoint local, bool mutationReplay)
    {
        _local = local;
        _dispatch = BinaryEndpoint.Create(services, mutationReplay);
    }

    internal static NativeTlsListener CreateListener(Socket socket, SslServerAuthenticationOptions tls, IServiceProvider services, bool mutationReplay, int capacity)
    {
        if (socket.LocalEndPoint is not IPEndPoint local)
            throw new InvalidOperationException("The public TLS endpoint must be bound before accepting clients.");
        var endpoint = new NativeBinaryEndpoint(services, local, mutationReplay);
        return new NativeTlsListener(socket, tls, endpoint.ServeAsync, services.GetRequiredService<ILogger<NativeTlsListener>>(), capacity);
    }

    private async Task ServeAsync(SslStream stream, IPEndPoint remote, CancellationToken graceful, CancellationToken abort)
    {
        var pipe = new StreamPipe(stream);
        using var connection = new BinaryConnection(pipe, _local, remote, graceful, abort);
        try
        {
            await _dispatch(connection).ConfigureAwait(false);
        }
        finally
        {
            await connection.CompleteTransportAsync().ConfigureAwait(false);
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
}
