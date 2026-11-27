using System;
using System.IO;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Runs the cache protocol on TLS sessions admitted by the node's public listener.</summary>
internal sealed class NativeBinaryEndpoint
{
    private readonly Func<BinaryConnection, Task> _dispatch;
    private readonly IPEndPoint _local;
    private readonly Func<string> _nextConnectionId;

    internal NativeBinaryEndpoint(IServiceProvider services, IPEndPoint local, bool mutationReplay, Func<string> nextConnectionId)
    {
        _local = local;
        _nextConnectionId = nextConnectionId;
        _dispatch = BinaryEndpoint.Create(services, mutationReplay);
    }

    internal static NativeTlsListener CreateListener(Socket socket, SslServerAuthenticationOptions tls, IServiceProvider services, bool mutationReplay, int capacity, Func<string> nextConnectionId)
    {
        if (socket.LocalEndPoint is not IPEndPoint local)
            throw new InvalidOperationException("The public TLS endpoint must be bound before accepting clients.");
        var endpoint = new NativeBinaryEndpoint(services, local, mutationReplay, nextConnectionId);
        return new NativeTlsListener(socket, tls, endpoint.ServeAsync, services.Require<IServerLoggerFactory>().CreateLogger<NativeTlsListener>(), capacity);
    }

    private async Task ServeAsync(SslStream stream, IPEndPoint remote, CancellationToken graceful, CancellationToken abort)
    {
        var pipe = new StreamPipe(stream);
        using var connection = new BinaryConnection(_nextConnectionId(), pipe, _local, remote, graceful, abort);
        try
        {
            await _dispatch(connection).ConfigureAwait(false);
        }
        finally
        {
            await connection.CompleteTransportAsync().ConfigureAwait(false);
        }
    }

    internal sealed class StreamPipe : IDuplexPipe
    {
        private static readonly StreamPipeReaderOptions ReaderOptions = new(leaveOpen: true);
        private static readonly StreamPipeWriterOptions WriterOptions = new(minimumBufferSize: Frames.MaxBatchBytes, leaveOpen: true);

        internal StreamPipe(Stream stream)
        {
            Input = PipeReader.Create(stream, ReaderOptions);
            Output = PipeWriter.Create(stream, WriterOptions);
        }

        public PipeReader Input { get; }

        public PipeWriter Output { get; }
    }
}
