using System;
using System.Buffers;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Endpoint;

internal static class PeerEndpoint
{
    internal static Task PingAsync(HttpContext http) => HandleAsync(http, static (_, body, _) =>
        Task.FromResult(body.IsEmpty ? new BinaryResponse(FrameStatus.Ok) : new BinaryResponse(FrameStatus.InvalidArgument)));

    internal static async Task HandleAsync(HttpContext http, Func<HttpContext, ReadOnlyMemory<byte>, string, Task<BinaryResponse>> handle)
    {
        if (!HttpProtocol.IsHttp2(http.Request.Protocol) || !string.Equals(http.Request.ContentType, PeerProtocol.MediaType, StringComparison.OrdinalIgnoreCase) ||
            http.Request.Headers.ContainsKey("Content-Encoding"))
        {
            http.Response.StatusCode = StatusCodes.Status415UnsupportedMediaType;
            return;
        }

        if (http.Request.ContentLength is not (>= 0 and <= EntryLimits.GrpcMaxReceiveMessageSizeBytes))
        {
            http.Response.StatusCode = StatusCodes.Status413PayloadTooLarge;
            return;
        }

        var verifier = http.RequestServices.GetRequiredService<VerifiedPeerCertificates>();
        if (http.Connection.ClientCertificate is not { } cert || !verifier.TryVerify(cert, out var peer))
        {
            http.Response.StatusCode = StatusCodes.Status403Forbidden;
            return;
        }

        var clock = http.RequestServices.GetRequiredService<TimeProvider>();
        TimeSpan? budget = null;
        if (http.Request.Headers.TryGetValue(PeerProtocol.BudgetHeader, out var header))
        {
            if (header.Count != 1 || !long.TryParse(header[0], NumberStyles.None, CultureInfo.InvariantCulture, out var ticks) || ticks <= 0)
            {
                http.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            budget = TimeSpan.FromTicks(ticks);
        }

        var reply = await ExecuteAsync(http, handle, peer, clock, budget).ConfigureAwait(false);
        await WriteAsync(http, reply).ConfigureAwait(false);
    }

    private static async Task<BinaryResponse> ExecuteAsync(HttpContext http, Func<HttpContext, ReadOnlyMemory<byte>, string, Task<BinaryResponse>> handle, string peer, TimeProvider clock, TimeSpan? budget)
    {
        using var deadlineScope = budget is { } left ? ServerRpcDeadlineContext.Push(new ServerDeadlineBudget(clock, left, clock.GetTimestamp())) : null;
        var aborted = http.RequestAborted;
        using var deadline = budget is { TotalMilliseconds: < uint.MaxValue - 1 } timeout ? new TimedCancellation(timeout, clock, aborted) : null;
        http.RequestAborted = deadline?.Token ?? aborted;
        var size = Convert.ToInt32(http.Request.ContentLength!.Value);
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        BinaryResponse reply;
        try
        {
            var body = buffer.AsMemory(0, size);
            await http.Request.Body.ReadExactlyAsync(body, http.RequestAborted).ConfigureAwait(false);
            reply = await handle(http, body, peer).ConfigureAwait(false);
        }
        catch (ServerCallException error)
        {
            reply = new BinaryResponse(error.StatusCode, error.Error);
        }
        catch (SquirixException error)
        {
            reply = BinaryResponse.FromError(error);
        }
        catch (InvalidProtocolBufferException)
        {
            reply = new BinaryResponse(FrameStatus.InvalidArgument, new BinaryCallError { Detail = "Malformed peer request." });
        }
        catch (OperationCanceledException) when (!aborted.IsCancellationRequested && deadline?.IsCancellationRequested == true)
        {
            reply = new BinaryResponse(FrameStatus.DeadlineExceeded, new BinaryCallError { Detail = "Request deadline exceeded." });
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
            http.RequestAborted = aborted;
        }

        return reply;
    }

    private static async Task WriteAsync(HttpContext http, BinaryResponse reply)
    {
        if (reply.Payload?.CalculateSize() > EntryLimits.GrpcMaxSendMessageSizeBytes)
            reply = new BinaryResponse(FrameStatus.ResourceExhausted, new BinaryCallError { Detail = "The response exceeds the peer message limit." });

        http.Response.ContentType = PeerProtocol.MediaType;
        http.Response.ContentLength = reply.Payload?.CalculateSize() ?? 0;
        http.Response.Headers[PeerProtocol.StatusHeader] = Unsafe.BitCast<FrameStatus, int>(reply.Status).ToString(CultureInfo.InvariantCulture);
        reply.Payload?.WriteTo(http.Response.BodyWriter);
        _ = await http.Response.BodyWriter.FlushAsync(http.RequestAborted).ConfigureAwait(false);
    }
}
