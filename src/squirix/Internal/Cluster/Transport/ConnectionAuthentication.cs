using System;
using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Transport.Wire;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Authenticates each physical TLS transport before the client can send cache operations.</summary>
internal static class ConnectionAuthentication
{
    private static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(5);

    internal static async ValueTask<Stream> AuthenticateAsync(Stream stream, Func<CancellationToken, ValueTask<string>> provider, CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(HandshakeTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        var token = await provider(linked.Token).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(token))
            throw new ConnectionRejectedException();
        var byteCount = Encoding.UTF8.GetByteCount(token);
        if (byteCount > ConnectionAuthenticationProtocol.MaximumTokenBytes)
            throw new ConnectionRejectedException();

        var header = ArrayPool<byte>.Shared.Rent(ConnectionAuthenticationProtocol.HeaderLength);
        var payload = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            ConnectionAuthenticationProtocol.WriteRequestHeader(header.AsSpan(0, ConnectionAuthenticationProtocol.HeaderLength), byteCount);
            await stream.WriteAsync(header.AsMemory(0, ConnectionAuthenticationProtocol.HeaderLength), linked.Token).ConfigureAwait(false);
            _ = Encoding.UTF8.GetBytes(token, payload);
            await stream.WriteAsync(payload.AsMemory(0, byteCount), linked.Token).ConfigureAwait(false);
            await stream.FlushAsync(linked.Token).ConfigureAwait(false);
            await stream.ReadExactlyAsync(header.AsMemory(0, ConnectionAuthenticationProtocol.HeaderLength), linked.Token).ConfigureAwait(false);
            return ReadAcknowledgement(header.AsSpan(0, ConnectionAuthenticationProtocol.HeaderLength), stream);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(header);
            ArrayPool<byte>.Shared.Return(payload, true);
        }
    }

    private static Stream ReadAcknowledgement(ReadOnlySpan<byte> header, Stream stream) => ReadStatus(header) switch
    {
        ConnectionAuthenticationProtocol.Accepted => stream,
        ConnectionAuthenticationProtocol.InvalidCredentials or ConnectionAuthenticationProtocol.UnsupportedVersion => throw new ConnectionRejectedException(),
        _ => throw new IOException("The server is busy authenticating connections."),
    };

    private static byte ReadStatus(ReadOnlySpan<byte> header) =>
        ConnectionAuthenticationProtocol.TryReadAcknowledgement(header, out var status)
            ? status
            : throw new IOException("The server returned an invalid connection authentication acknowledgement.");
}
