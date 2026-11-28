using System;
using System.Net.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Core;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Exercises cache operations through the RESP listener and shared server pipeline.</summary>
public sealed class RespCacheTests
{
    /// <summary>Commands in a pipeline observe state changes in request order in both modes.</summary>
    /// <param name="resp3">Whether the connection negotiates RESP3.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(30_000)]
    public async Task PipelinedReadsSeeOnlyEarlierWrites(bool resp3, CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        await using var stream = await host.ConnectAsync(cancellationToken);
        if (resp3)
            await NegotiateRespThreeAsync(stream, cancellationToken);
        var requests = RespTestHost.Command("SET", "x", "1") + RespTestHost.Command("GET", "x") + RespTestHost.Command("SET", "x", "2") +
                       RespTestHost.Command("GET", "x") + RespTestHost.Command("DEL", "x", "other") + RespTestHost.Command("EXISTS", "x") +
                       RespTestHost.Command("UNLINK", "x") + RespTestHost.Command("DEL", "x") + RespTestHost.Command("GET", "x");
        var expected = "+OK\r\n$1\r\n1\r\n+OK\r\n$1\r\n2\r\n-ERR wrong number of arguments\r\n:1\r\n:1\r\n:0\r\n" +
                       (resp3 ? "_\r\n" : "$-1\r\n");
        await ExchangeAsync(stream, requests, expected, cancellationToken);
    }

    /// <summary>Only the explicit RESP namespace shares bytes with the native cache route.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task StoresBytesInIsolatedNativeNamespace(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        await using var stream = await host.ConnectAsync(cancellationToken);
        byte[] payload = [0, 255, 13, 10];
        byte[] request = [.. "*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$4\r\n"u8, .. payload, .. "\r\n"u8];
        await stream.WriteAsync(request, cancellationToken);
        await ReadReplyAsync(stream, "+OK\r\n", cancellationToken);

        var native = host.Server.Services.GetRequiredService<RoutedCacheOperations<StoredValue>>();
        var isolated = await native.Reads.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "x" }, cancellationToken);
        _ = await Assert.That(isolated.Found).IsFalse();
        var shared = await native.Reads.GetValueAsync(new GetValueAsyncRequest { CacheName = "resp", Key = "x" }, cancellationToken);
        _ = await Assert.That(shared.Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.BinaryValue);
        _ = await Assert.That(shared.Value.BinaryValue.Span.SequenceEqual(payload)).IsTrue();

        _ = await WriteNativeAsync(native, "binary", new CacheValue { BinaryValue = payload }, cancellationToken);
        _ = await WriteNativeAsync(native, "otherKind", new CacheValue { StringValue = "string" }, cancellationToken);
        await stream.WriteAsync(Encoding.UTF8.GetBytes(RespTestHost.Command("GET", "binary")), cancellationToken);
        byte[] expected = [.. "$4\r\n"u8, .. payload, .. "\r\n"u8];
        var result = new byte[expected.Length];
        await stream.ReadExactlyAsync(result, cancellationToken);
        _ = await Assert.That(result.AsSpan().SequenceEqual(expected)).IsTrue();
        await ExchangeAsync(stream, RespTestHost.Command("GET", "otherKind"), "-WRONGTYPE operation against a key holding the wrong kind of value\r\n", cancellationToken);
    }

    /// <summary>Local RESP reads share the stored snapshot and retain it across an overwrite.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task ReadsShareSnapshotAcrossOverwrite(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        var cache = host.Server.Services.GetRequiredService<RoutedCacheOperations<StoredValue>>();
        var request = new GetValueAsyncRequest { CacheName = "resp", Key = "snapshot" };
        byte[] original = [1, 0, 255, 13, 10];
        byte[] updated = [2, 0, 13, 10];
        _ = await WriteNativeAsync(cache, request.Key, new CacheValue { BinaryValue = original }, cancellationToken);

        var first = await cache.Reads.GetRespValueAsync(request, cancellationToken);
        var second = await cache.Reads.GetRespValueAsync(request, cancellationToken);
        _ = await WriteNativeAsync(cache, request.Key, new CacheValue { BinaryValue = updated }, cancellationToken);
        var replacement = await cache.Reads.GetRespValueAsync(request, cancellationToken);

        _ = await Assert.That(first.Value!.BinaryValue.Span.Overlaps(second.Value!.BinaryValue.Span, out var offset)).IsTrue();
        _ = await Assert.That(offset).IsEqualTo(0);
        _ = await Assert.That(first.Value.BinaryValue.Span.SequenceEqual(original)).IsTrue();
        _ = await Assert.That(replacement.Value!.BinaryValue.Span.SequenceEqual(updated)).IsTrue();
    }

    /// <summary>Malformed UTF-8 and oversized keys cannot alias valid cache entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task ValidatesKeysWithoutNormalization(CancellationToken cancellationToken)
    {
        await using var host = await RespTestHost.StartAsync(cancellationToken);
        await using var stream = await host.ConnectAsync(cancellationToken);
        var largestKey = new string('界', 1024);
        var validMaximum = RespTestHost.Command("SET", largestKey, "max") + RespTestHost.Command("GET", largestKey);
        await ExchangeAsync(stream, validMaximum, "+OK\r\n$3\r\nmax\r\n", cancellationToken);
        var writes = RespTestHost.Command("SET", largestKey + "界", "no") + RespTestHost.Command("SET", "\u0000", "no") +
                     RespTestHost.Command("SET", "�", "ok") + RespTestHost.Command("SET", "é", "one") + RespTestHost.Command("SET", "e\u0301", "two");
        await ExchangeAsync(stream, writes, "-ERR invalid cache key\r\n-ERR invalid cache key\r\n+OK\r\n+OK\r\n+OK\r\n", cancellationToken);
        byte[] invalid = [.. "*3\r\n$3\r\nSET\r\n$1\r\n"u8, 255, .. "\r\n$3\r\nbad\r\n"u8];
        await stream.WriteAsync(invalid, cancellationToken);
        await ReadReplyAsync(stream, "-ERR invalid cache key\r\n", cancellationToken);
        var reads = RespTestHost.Command("GET", "�") + RespTestHost.Command("GET", "é") + RespTestHost.Command("GET", "e\u0301");
        await ExchangeAsync(stream, reads, "$2\r\nok\r\n$3\r\none\r\n$3\r\ntwo\r\n", cancellationToken);
    }

    /// <summary>Denied writes never reach the cache, even after a client presents a valid token.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task AppliesNativeAuthorizationToCache(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var host = await RespTestHost.StartAsync(cancellationToken, jwt: credentials, register: static services =>
        {
            _ = services.AddSingleton<IBinaryAuthorizationHook>(new DenyWrites());
        });
        await using var stream = await host.ConnectAsync(cancellationToken);
        var token = TestJwtHelper.CreateBearerToken(credentials, "writer");
        var commands = RespTestHost.Command("SET", "unauthenticated", "no") + RespTestHost.Command("AUTH", "default", token) +
                       RespTestHost.Command("SET", "denied", "no") + RespTestHost.Command("GET", "denied") + RespTestHost.Command("GET", "unauthenticated");

        await ExchangeAsync(stream, commands, "-NOAUTH Authentication required.\r\n+OK\r\n-NOPERM cache operation denied\r\n$-1\r\n$-1\r\n", cancellationToken);
    }

    /// <summary>An overloaded read reports a server error instead of looking like a cache miss.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task ReportsAdmissionErrorsInsteadOfMisses(CancellationToken cancellationToken)
    {
        var admission = IBackpressureGate.Mock(MockBehavior.Strict);
        _ = admission.AcquireAsync("cache", "get", "conn:resp:1", Arg.Any<CancellationToken>())
                     .ReturnsAsync(ValueTask.FromResult((Decision.Rejected("busy"), Lease.Empty)));
        var gate = admission.Object;
        await using var host = await RespTestHost.StartAsync(cancellationToken, configure: static options => options.Backpressure.PerClientMaxInFlight = 1, register: services =>
        {
            _ = services.AddSingleton<IBackpressureGate>(gate);
        });
        await using var stream = await host.ConnectAsync(cancellationToken);

        await ExchangeAsync(stream, RespTestHost.Command("GET", "missing") + RespTestHost.Command("PING"), "-BUSY server is busy\r\n+PONG\r\n", cancellationToken);
    }

    internal static async Task ExchangeAsync(SslStream stream, string commands, string expected, CancellationToken cancellationToken)
    {
        await stream.WriteAsync(Encoding.UTF8.GetBytes(commands), cancellationToken);
        await ReadReplyAsync(stream, expected, cancellationToken);
    }

    private static Task NegotiateRespThreeAsync(SslStream stream, CancellationToken cancellationToken) => ExchangeAsync(
        stream,
        RespTestHost.Command("HELLO", "3"),
        RespTestHost.FirstHelloReply(3),
        cancellationToken);

    private static async Task ReadReplyAsync(SslStream stream, string expected, CancellationToken cancellationToken)
    {
        var bytes = new byte[Encoding.UTF8.GetByteCount(expected)];
        await stream.ReadExactlyAsync(bytes, cancellationToken);
        _ = await Assert.That(Encoding.UTF8.GetString(bytes)).IsEqualTo(expected);
    }

    private static ValueTask<SetAsyncResponse> WriteNativeAsync(RoutedCacheOperations<StoredValue> native, string key, CacheValue value, CancellationToken cancellationToken) =>
        native.Mutations.SetEntryAsync(new SetEntryAsyncRequest { CacheName = "resp", Key = key, Entry = new CacheEntryWire { Value = value } }, cancellationToken);

    private sealed class DenyWrites : IBinaryAuthorizationHook
    {
        public ValueTask<FrameStatus> AuthorizeAsync(BinaryRequestContext request) =>
            ValueTask.FromResult(request.User?.Identity?.IsAuthenticated == true && request.Path.EndsWith("/GetValue", StringComparison.Ordinal)
                ? FrameStatus.Ok
                : FrameStatus.PermissionDenied);
    }
}
