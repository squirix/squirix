using System;
using System.Buffers;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster.Transport;

internal static class PeerProtocol
{
    internal const string Prefix = "/squirix/peer/v1/";
    internal const string MediaType = "application/x-protobuf";
    internal const string BudgetHeader = "squirix-budget-ticks";
    internal const string StatusHeader = "squirix-status";

    internal static async Task<T> SendAsync<T>(HttpClient client, string path, IMessage request, MessageParser<T> parser, string? fingerprint, CancellationToken cancellationToken)
        where T : IMessage<T>
    {
        var length = request.CalculateSize();
        if (length > EntryLimits.GrpcMaxSendMessageSizeBytes)
            throw new ServerCallException(FrameStatus.ResourceExhausted, "The request exceeds the peer message limit.");

        using var message = new HttpRequestMessage(HttpMethod.Post, Prefix + path)
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new RequestContent(request, length),
        };
        message.Content.Headers.ContentType = new MediaTypeHeaderValue(MediaType);
        if (fingerprint != null)
            message.Headers.Add(RemoteInvocationContract.RingFingerprintHeaderName, fingerprint);
        if (ServerRpcDeadlineContext.CurrentBudget is { } budget)
        {
            var remaining = budget.Remaining;
            if (remaining <= TimeSpan.Zero)
                throw new ServerCallException(FrameStatus.DeadlineExceeded, "Request deadline exceeded.");
            message.Headers.Add(BudgetHeader, remaining.Ticks.ToString(CultureInfo.InvariantCulture));
        }

        using var response = await SendRequestAsync(client, message, cancellationToken).ConfigureAwait(false);
        if (response.StatusCode != HttpStatusCode.OK || response.Version.Major != 2 ||
            !string.Equals(response.Content.Headers.ContentType?.MediaType, MediaType, StringComparison.OrdinalIgnoreCase) ||
            !response.Headers.TryGetValues(StatusHeader, out var values))
            throw new ServerCallException(FrameStatus.Unavailable, $"Invalid peer response for {path} (HTTP {response.Version} {response.StatusCode}, content type {response.Content.Headers.ContentType}).");

        using var headers = values.GetEnumerator();
        if (!headers.MoveNext() || !int.TryParse(headers.Current, NumberStyles.None, CultureInfo.InvariantCulture, out var code) ||
            code is < 0 or > 16 || headers.MoveNext())
            throw new ServerCallException(FrameStatus.Internal, "The peer returned an invalid status.");

        if (response.Content.Headers.ContentLength is not (>= 0 and <= EntryLimits.GrpcMaxReceiveMessageSizeBytes) || response.Content.Headers.ContentEncoding.Count != 0)
            throw new ServerCallException(FrameStatus.Internal, "The peer returned an invalid payload length or encoding.");

        var size = Convert.ToInt32(response.Content.Headers.ContentLength.Value);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            await stream.ReadExactlyAsync(buffer.AsMemory(0, size), cancellationToken).ConfigureAwait(false);
            var status = Unsafe.BitCast<int, FrameStatus>(code);
            return status == FrameStatus.Ok ? parser.ParseFrom(buffer, 0, size) : throw new ServerCallException(status, BinaryCallError.Parser.ParseFrom(buffer, 0, size));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static async Task<HttpResponseMessage> SendRequestAsync(HttpClient client, HttpRequestMessage message, CancellationToken cancellationToken)
    {
        try
        {
            return await client.SendAsync(message, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRequestException error)
        {
            // A lost response may follow a committed mutation. Only failures before writing the body may prove a failed dial.
            var cause = message.Content is RequestContent { Started: false } ? error : null;
            throw new ServerCallException(FrameStatus.Unavailable, "Peer request failed.", cause);
        }
    }

    private sealed class RequestContent : HttpContent
    {
        private readonly IMessage _message;
        private readonly int _length;
        private int _started;

        internal RequestContent(IMessage message, int length)
        {
            _message = message;
            _length = length;
        }

        internal bool Started => Volatile.Read(ref _started) != 0;

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
            SerializeToStreamAsync(stream, context, CancellationToken.None);

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
        {
            var bytes = ArrayPool<byte>.Shared.Rent(_length);
            try
            {
                _message.WriteTo(bytes.AsSpan(0, _length));
                Volatile.Write(ref _started, 1);
                await stream.WriteAsync(bytes.AsMemory(0, _length), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bytes, true);
            }
        }

        protected override bool TryComputeLength(out long length)
        {
            length = _length;
            return true;
        }
    }
}
