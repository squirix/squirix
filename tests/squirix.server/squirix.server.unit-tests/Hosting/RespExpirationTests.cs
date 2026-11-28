using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using Squirix.Transport.Binary;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Tests expiry semantics through the complete RESP listener and both storage modes.</summary>
public sealed class RespExpirationTests : IsolatedStorageTestBase
{
    /// <summary>Reads and updates expiries using a deterministic node clock.</summary>
    /// <param name="persistent">Whether to enable the journal.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(30_000)]
    public async Task ReportsAndChangesLiveExpirations(bool persistent, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var host = await StartAsync(clock, persistent, cancellationToken);
        await using var stream = await host.ConnectAsync(cancellationToken);
        var first = RespTestHost.Command("TTL", "x") + RespTestHost.Command("SET", "x", "v") + RespTestHost.Command("PTTL", "x") +
                    RespTestHost.Command("PEXPIRE", "x", "1500") + RespTestHost.Command("TTL", "x") + RespTestHost.Command("PTTL", "x");
        await RespCacheTests.ExchangeAsync(stream, first, ":-2\r\n+OK\r\n:-1\r\n:1\r\n:2\r\n:1500\r\n", cancellationToken);

        clock.Advance(TimeSpan.FromMilliseconds(1));
        var second = RespTestHost.Command("TTL", "x") + RespTestHost.Command("EXPIRE", "x", "30") +
                     RespTestHost.Command("PTTL", "x") + RespTestHost.Command("PERSIST", "x") + RespTestHost.Command("PERSIST", "x") +
                     RespTestHost.Command("TTL", "x") + RespTestHost.Command("EXPIRE", "x", "-1") + RespTestHost.Command("GET", "x") +
                     RespTestHost.Command("PEXPIRE", "x", "0") + RespTestHost.Command("PERSIST", "x");
        await RespCacheTests.ExchangeAsync(stream, second, ":1\r\n:1\r\n:30000\r\n:1\r\n:0\r\n:-1\r\n:1\r\n$-1\r\n:0\r\n:0\r\n", cancellationToken);
    }

    /// <summary>Rejects invalid durations and options before changing an existing entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task InvalidExpiryLeavesValueAndDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var host = await StartAsync(clock, false, cancellationToken);
        await using var stream = await host.ConnectAsync(cancellationToken);
        var commands = RespTestHost.Command("SET", "x", "old", "EX", "12") + RespTestHost.Command("SET", "x", "new", "PX", "-1") +
                       RespTestHost.Command("SETEX", "x", "01", "new") + RespTestHost.Command("PSETEX", "x", "9223372036854775807", "new") +
                       RespTestHost.Command("SET", "x", "new", "EX", "1", "PX", "2") + RespTestHost.Command("EXPIRE", "x", "120x") +
                       RespTestHost.Command("PEXPIRE", "x", "0", "XX") + RespTestHost.Command("GET", "x") + RespTestHost.Command("PTTL", "x");
        const string expected = "+OK\r\n-ERR invalid expire time\r\n-ERR invalid expire time\r\n-ERR invalid expire time\r\n-ERR syntax error\r\n" +
                                "-ERR invalid expire time\r\n-ERR wrong number of arguments\r\n$3\r\nold\r\n:12000\r\n";
        await RespCacheTests.ExchangeAsync(stream, commands, expected, cancellationToken);
    }

    /// <summary>Restores the exact deadlines of acknowledged SETEX and PSETEX writes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task RestoresAcknowledgedDeadlines(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using (var initial = await StartAsync(clock, true, cancellationToken))
        await using (var first = await initial.ConnectAsync(cancellationToken))
        {
            var writes = RespTestHost.Command("SETEX", "seconds", "2", "s") + RespTestHost.Command("PSETEX", "milliseconds", "100", "m");
            await RespCacheTests.ExchangeAsync(first, writes, "+OK\r\n+OK\r\n", cancellationToken);
        }

        clock.Advance(TimeSpan.FromMilliseconds(99));
        await using var restored = await StartAsync(clock, true, cancellationToken);
        await using var stream = await restored.ConnectAsync(cancellationToken);
        var queries = RespTestHost.Command("PTTL", "seconds") + RespTestHost.Command("PTTL", "milliseconds") + RespTestHost.Command("GET", "milliseconds");
        await RespCacheTests.ExchangeAsync(stream, queries, ":1901\r\n:1\r\n$1\r\nm\r\n", cancellationToken);
        clock.Advance(TimeSpan.FromMilliseconds(1));
        var removed = RespTestHost.Command("GET", "milliseconds") + RespTestHost.Command("PTTL", "milliseconds");
        await RespCacheTests.ExchangeAsync(stream, removed, "$-1\r\n:-2\r\n", cancellationToken);
        var reset = RespTestHost.Command("SET", "seconds", "new") + RespTestHost.Command("TTL", "seconds");
        await RespCacheTests.ExchangeAsync(stream, reset, "+OK\r\n:-1\r\n", cancellationToken);
    }

    /// <summary>Immediate expiration is authorized as a removal before it can mutate storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task ImmediateExpiryNeedsRemovePermission(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var host = await RespTestHost.StartAsync(cancellationToken, jwt: credentials, register: static services =>
        {
            _ = services.AddSingleton<IBinaryAuthorizationHook>(new DenyRemoval());
        });
        await using var stream = await host.ConnectAsync(cancellationToken);
        var commands = RespTestHost.Command("AUTH", TestJwtHelper.CreateBearerToken(credentials)) + RespTestHost.Command("SET", "x", "kept") +
                       RespTestHost.Command("EXPIRE", "x", "120") + RespTestHost.Command("EXPIRE", "x", "-1") + RespTestHost.Command("PEXPIRE", "x", "0") +
                       RespTestHost.Command("GET", "x");
        const string expected = "+OK\r\n+OK\r\n:1\r\n-NOPERM cache operation denied\r\n-NOPERM cache operation denied\r\n$4\r\nkept\r\n";
        await RespCacheTests.ExchangeAsync(stream, commands, expected, cancellationToken);
    }

    private ValueTask<RespTestHost> StartAsync(TimeProvider clock, bool persistent, CancellationToken cancellationToken) =>
        RespTestHost.StartAsync(
            cancellationToken,
            configure: options =>
            {
                if (persistent)
                    options.UsePersistence(Dir);
            },
            register: services =>
            {
                _ = services.AddSingleton(clock);
            });

    private sealed class DenyRemoval : IBinaryAuthorizationHook
    {
        public ValueTask<FrameStatus> AuthorizeAsync(BinaryRequestContext request) =>
            ValueTask.FromResult(request.Path.EndsWith("/Remove", StringComparison.Ordinal) ? FrameStatus.PermissionDenied : FrameStatus.Ok);
    }
}
