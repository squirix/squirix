using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.IntegrationTests.Support;

internal sealed class PeerSocketProbe
{
    private readonly Uri _address;
    private readonly SslClientAuthenticationOptions _tls;

    internal PeerSocketProbe(Uri address, SslClientAuthenticationOptions tls)
    {
        _address = address;
        _tls = tls;
    }

    internal Task<GetValueAsyncResponse> GetValueAsync(GetValueAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync("owner/GetValue", request, GetValueAsyncResponse.Parser, headers, cancellationToken);

    internal Task<GetEntryAsyncResponse> GetEntryAsync(GetEntryAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync("owner/GetEntry", request, GetEntryAsyncResponse.Parser, headers, cancellationToken);

    internal Task<GetExpirationAsyncResponse> GetExpirationAsync(GetExpirationAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync("owner/GetExpiration", request, GetExpirationAsyncResponse.Parser, headers, cancellationToken);

    internal Task<SetAsyncResponse> SetEntryAsync(SetEntryAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync("owner/SetEntry", request, SetAsyncResponse.Parser, headers, cancellationToken);

    internal Task<TryAddAsyncResponse> AddEntryIfAbsentAsync(TryAddEntryAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync("owner/TryAddEntry", request, TryAddAsyncResponse.Parser, headers, cancellationToken);

    internal Task<GetReplicaStatusResponse> GetReplicaStatusAsync(GetReplicaStatusRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync("replication/GetReplicaStatus", request, GetReplicaStatusResponse.Parser, headers, cancellationToken);

    internal Task<ReplicaVoteResponse> PreVoteAsync(ReplicaVoteRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync("replication/PreVote", request, ReplicaVoteResponse.Parser, headers, cancellationToken);

    internal Task<ReplicaVoteResponse> RequestVoteAsync(ReplicaVoteRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync("replication/RequestVote", request, ReplicaVoteResponse.Parser, headers, cancellationToken);

    internal Task<AppendReplicaEntriesResponse> AppendReplicaEntriesAsync(AppendReplicaEntriesRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync("replication/AppendReplicaEntries", request, AppendReplicaEntriesResponse.Parser, headers, cancellationToken);

    internal async Task<T> SendAsync<T>(string path, IMessage payload, MessageParser<T> parser, Dictionary<string, string>? headers, CancellationToken cancellationToken)
        where T : IMessage<T>
    {
        var (status, body) = await SendRawAsync(path, payload.ToByteArray(), headers, -1, false, cancellationToken);
        return status == FrameStatus.Ok ? parser.ParseFrom(body) : throw new ServerCallException(status, BinaryCallError.Parser.ParseFrom(body));
    }

    internal async Task<(FrameStatus Status, byte[] Body)> SendRawAsync(string path, byte[] body, Dictionary<string, string>? headers, long budget, bool unsupportedVersion, CancellationToken cancellationToken)
    {
        using var socket = new TcpClient();
        await socket.ConnectAsync(_address.DnsSafeHost, _address.Port, cancellationToken);
        await using var stream = new SslStream(socket.GetStream());
        _tls.TargetHost = _address.DnsSafeHost;
        _tls.ApplicationProtocols = [PeerFrames.Protocol];
        try
        {
            await stream.AuthenticateAsClientAsync(_tls, cancellationToken);
        }
        catch (System.Security.Authentication.AuthenticationException error)
        {
            throw new IOException("The peer refused the TLS handshake.", error);
        }

        if (stream.NegotiatedApplicationProtocol != PeerFrames.Protocol)
            throw new IOException("The peer protocol was not negotiated.");

        var fingerprint = headers?.GetValueOrDefault("squirix-ring-fingerprint");
        var parent = headers?.GetValueOrDefault("traceparent");
        var state = headers?.GetValueOrDefault("tracestate");
        var metadata = PeerFrames.Text.GetBytes(string.Concat(path, fingerprint, parent, state));
        var frame = new PeerFrames.Request(body.Length, budget, PeerFrames.Text.GetByteCount(path), PeerFrames.Text.GetByteCount(fingerprint ?? string.Empty), PeerFrames.Text.GetByteCount(parent ?? string.Empty), PeerFrames.Text.GetByteCount(state ?? string.Empty));
        var bytes = new byte[PeerFrames.RequestBytes + metadata.Length + body.Length];
        PeerFrames.WriteRequest(bytes, in frame);
        if (unsupportedVersion)
            bytes[3]++;
        metadata.CopyTo(bytes, PeerFrames.RequestBytes);
        body.CopyTo(bytes, PeerFrames.RequestBytes + metadata.Length);
        await stream.WriteAsync(bytes, cancellationToken);

        var header = bytes.AsMemory(0, PeerFrames.ReplyBytes);
        await stream.ReadExactlyAsync(header, cancellationToken);
        var (code, size) = PeerFrames.ReadReply(header.Span);
        var result = new byte[size];
        await stream.ReadExactlyAsync(result, cancellationToken);
        return (Unsafe.BitCast<int, FrameStatus>(code), result);
    }
}
