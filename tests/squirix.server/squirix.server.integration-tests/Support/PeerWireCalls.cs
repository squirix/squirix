using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.IntegrationTests.Support;

internal static class PeerWireCalls
{
    internal static Task<GetValueAsyncResponse> GetValueAsync(this HttpClient client, GetValueAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync(client, "owner/GetValue", request, GetValueAsyncResponse.Parser, headers, cancellationToken);

    internal static Task<GetEntryAsyncResponse> GetEntryAsync(this HttpClient client, GetEntryAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync(client, "owner/GetEntry", request, GetEntryAsyncResponse.Parser, headers, cancellationToken);

    internal static Task<GetExpirationAsyncResponse> GetExpirationAsync(this HttpClient client, GetExpirationAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync(client, "owner/GetExpiration", request, GetExpirationAsyncResponse.Parser, headers, cancellationToken);

    internal static Task<SetAsyncResponse> SetEntryAsync(this HttpClient client, SetEntryAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync(client, "owner/SetEntry", request, SetAsyncResponse.Parser, headers, cancellationToken);

    internal static Task<TryAddAsyncResponse> AddEntryIfAbsentAsync(this HttpClient client, TryAddEntryAsyncRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync(client, "owner/TryAddEntry", request, TryAddAsyncResponse.Parser, headers, cancellationToken);

    internal static Task<GetReplicaStatusResponse> GetReplicaStatusAsync(this HttpClient client, GetReplicaStatusRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync(client, "replication/GetReplicaStatus", request, GetReplicaStatusResponse.Parser, headers, cancellationToken);

    internal static Task<ReplicaVoteResponse> PreVoteAsync(this HttpClient client, ReplicaVoteRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync(client, "replication/PreVote", request, ReplicaVoteResponse.Parser, headers, cancellationToken);

    internal static Task<ReplicaVoteResponse> RequestVoteAsync(this HttpClient client, ReplicaVoteRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync(client, "replication/RequestVote", request, ReplicaVoteResponse.Parser, headers, cancellationToken);

    internal static Task<AppendReplicaEntriesResponse> AppendReplicaEntriesAsync(this HttpClient client, AppendReplicaEntriesRequest request, Dictionary<string, string>? headers = null, CancellationToken cancellationToken = default) =>
        SendAsync(client, "replication/AppendReplicaEntries", request, AppendReplicaEntriesResponse.Parser, headers, cancellationToken);

    private static async Task<T> SendAsync<T>(HttpClient client, string path, WireMessage payload, WireParser<T> parser, Dictionary<string, string>? headers, CancellationToken cancellationToken)
        where T : WireMessage
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, PeerProtocol.Prefix + path)
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new ByteArrayContent(payload.ToByteArray()),
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        if (headers != null)
        {
            foreach (var header in headers)
                request.Headers.Add(header.Key, header.Value);
        }

        using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
        _ = response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        using var statuses = response.Headers.GetValues("squirix-status").GetEnumerator();
        if (!statuses.MoveNext())
            throw new InvalidOperationException("Missing peer status.");
        var code = int.Parse(statuses.Current, CultureInfo.InvariantCulture);
        return code == 0 ? parser.ParseFrom(body) : throw new ServerCallException(Unsafe.BitCast<int, FrameStatus>(code), BinaryCallError.Parser.ParseFrom(body));
    }
}
