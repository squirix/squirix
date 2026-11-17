using System;
using System.Buffers;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Transport.Grpc;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Authenticates a raw test channel's physical transport before HTTP/2 starts.</summary>
internal sealed class ConnectionAuthenticationTransport
{
    private readonly Action<byte>? _acknowledgement;
    private readonly string _token;

    private ConnectionAuthenticationTransport(string token, Action<byte>? acknowledgement)
    {
        _token = token;
        _acknowledgement = acknowledgement;
    }

    /// <summary>Installs one authentication exchange for each new TLS transport.</summary>
    /// <param name="handler">The unused handler owned by the test channel.</param>
    /// <param name="token">The credential to present.</param>
    /// <param name="acknowledgement">Optional observer of the server's explicit reply status.</param>
    internal static void Configure(SocketsHttpHandler handler, string token, Action<byte>? acknowledgement = null)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentException.ThrowIfNullOrEmpty(token);
        var transport = new ConnectionAuthenticationTransport(token, acknowledgement);
        handler.PlaintextStreamFilter = transport.AuthenticateAsync;
    }

    private async ValueTask<Stream> AuthenticateAsync(SocketsHttpPlaintextStreamFilterContext context, CancellationToken cancellationToken)
    {
        var tokenBytes = Encoding.UTF8.GetBytes(_token);
        var header = ArrayPool<byte>.Shared.Rent(ConnectionAuthenticationProtocol.HeaderLength);
        try
        {
            var headerMemory = header.AsMemory(0, ConnectionAuthenticationProtocol.HeaderLength);
            ConnectionAuthenticationProtocol.WriteRequestHeader(headerMemory.Span, tokenBytes.Length);
            await context.PlaintextStream.WriteAsync(headerMemory, cancellationToken).ConfigureAwait(false);
            await context.PlaintextStream.WriteAsync(tokenBytes, cancellationToken).ConfigureAwait(false);
            await context.PlaintextStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            await context.PlaintextStream.ReadExactlyAsync(headerMemory, cancellationToken).ConfigureAwait(false);
            if (!ConnectionAuthenticationProtocol.TryReadAcknowledgement(headerMemory.Span, out var status))
                throw new IOException("The test server returned an invalid connection authentication acknowledgement.");
            _acknowledgement?.Invoke(status);
            return status == ConnectionAuthenticationProtocol.Accepted
                ? context.PlaintextStream
                : throw new IOException("The test server rejected connection authentication.");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(header);
        }
    }
}
