using System;
using System.IO;
using System.Net.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies conditional RESP writes through the node and alongside native operations.</summary>
public sealed class RespConditionalWriteTests : IsolatedStorageTestBase
{
    /// <summary>Two connections compete for one absent key and exactly one wins.</summary>
    /// <param name="persistent">Whether to enable the journal.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(30_000)]
    public async Task ConcurrentInsertHasOneWinner(bool persistent, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var host = await StartAsync(clock, persistent, cancellationToken);
        await using var first = await host.ConnectAsync(cancellationToken);
        await using var second = await host.ConnectAsync(cancellationToken);
        var writes = await Task.WhenAll(
            WriteAndReadLineAsync(first, RespTestHost.Command("SET", "x", "one", "NX", "PX", "1100"), cancellationToken),
            WriteAndReadLineAsync(second, RespTestHost.Command("SET", "x", "two", "PX", "2200", "NX"), cancellationToken));

        _ = await Assert.That(string.Equals(writes[0], "+OK", StringComparison.Ordinal) ? writes[1] : writes[0]).IsEqualTo("$-1");
        _ = await Assert.That(string.Equals(writes[0], "$-1", StringComparison.Ordinal) ? writes[1] : writes[0]).IsEqualTo("+OK");
        var expected = string.Equals(writes[0], "+OK", StringComparison.Ordinal) ? "$3\r\none\r\n:1100\r\n" : "$3\r\ntwo\r\n:2200\r\n";
        await RespCacheTests.ExchangeAsync(first, RespTestHost.Command("GET", "x") + RespTestHost.Command("PTTL", "x"), expected, cancellationToken);
    }

    /// <summary>Failures preserve the current value and deadline and write no journal frame.</summary>
    /// <param name="persistent">Whether to enable the journal.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(30_000)]
    public async Task FailedConditionsPreserveState(bool persistent, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var host = await StartAsync(clock, persistent, cancellationToken);
        await using var stream = await host.ConnectAsync(cancellationToken);
        await RespCacheTests.ExchangeAsync(stream, RespTestHost.Command("SET", "live", "old", "PX", "1200"), "+OK\r\n", cancellationToken);
        var journal = persistent ? host.Server.Services.GetRequiredService<IJournalCoordinator>() : null;
        var before = journal?.AppendedOps;
        var failures = RespTestHost.Command("SET", "live", "new", "NX", "EX", "5") + RespTestHost.Command("SET", "absent", "new", "XX", "EX", "5") +
                       RespTestHost.Command("SET", "live", "new", "NX", "XX") + RespTestHost.Command("SETNX", "live", "new") +
                       RespTestHost.Command("GET", "live") + RespTestHost.Command("PTTL", "live");
        await RespCacheTests.ExchangeAsync(stream, failures, "$-1\r\n$-1\r\n-ERR syntax error\r\n:0\r\n$3\r\nold\r\n:1200\r\n", cancellationToken);
        _ = await Assert.That(journal?.AppendedOps).IsEqualTo(before);

        clock.Advance(TimeSpan.FromMilliseconds(1200));
        var afterExpiry = RespTestHost.Command("SET", "live", "new", "XX") + RespTestHost.Command("SETNX", "live", "created") +
                          RespTestHost.Command("SET", "live", "updated", "XX") + RespTestHost.Command("PTTL", "live") + RespTestHost.Command("GET", "live");
        await RespCacheTests.ExchangeAsync(stream, afterExpiry, "$-1\r\n:1\r\n+OK\r\n:-1\r\n$7\r\nupdated\r\n", cancellationToken);
    }

    /// <summary>A competing native writer cannot leave the value and TTL from different requests.</summary>
    /// <param name="persistent">Whether to enable the journal.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(30_000)]
    public async Task ReplacementPairsValueAndDeadline(bool persistent, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var host = await StartAsync(clock, persistent, cancellationToken);
        await using var stream = await host.ConnectAsync(cancellationToken);
        await RespCacheTests.ExchangeAsync(stream, RespTestHost.Command("SET", "x", "old", "PX", "1200"), "+OK\r\n", cancellationToken);
        var native = host.Server.Services.GetRequiredService<RoutedCacheOperations<StoredValue>>();
        var request = new SetEntryAsyncRequest
        {
            CacheName = "resp",
            Key = "x",
            Entry = new CacheEntryWire { Value = new CacheValue { BinaryValue = new byte[] { 110 } }, Expiration = TimeSpan.FromMilliseconds(1800) },
        };
#pragma warning disable CA2025 // The finally block awaits the RESP exchange before disposing the stream.
        var resp = RespCacheTests.ExchangeAsync(stream, RespTestHost.Command("SET", "x", "r", "XX", "PX", "3600"), "+OK\r\n", cancellationToken);
#pragma warning restore CA2025
        try
        {
            _ = await native.Mutations.SetEntryAsync(request, cancellationToken);
        }
        finally
        {
            await resp;
        }

        var lookup = new GetEntryAsyncRequest { CacheName = "resp", Key = "x" };
        var pair = await native.Reads.GetEntryAsync(lookup, cancellationToken);
        var value = Encoding.ASCII.GetString(pair.Entry!.Value.Value!.BinaryValue.Span);
        (string Value, TimeSpan? Remaining) expected = string.Equals(value, "r", StringComparison.Ordinal) ? ("r", TimeSpan.FromMilliseconds(3600)) : ("n", TimeSpan.FromMilliseconds(1800));
        _ = await Assert.That((value, pair.Remaining)).IsEqualTo(expected);
    }

    /// <summary>Replacement acknowledges the value and exact deadline as one recoverable put.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task ReplacementDeadlineSurvivesRestart(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using (var first = await StartAsync(clock, true, cancellationToken))
        await using (var stream = await first.ConnectAsync(cancellationToken))
        {
            var writes = RespTestHost.Command("SET", "x", "old", "NX", "EX", "120") + RespTestHost.Command("SET", "x", "new", "XX", "PX", "3100");
            await RespCacheTests.ExchangeAsync(stream, writes, "+OK\r\n+OK\r\n", cancellationToken);
        }

        clock.Advance(TimeSpan.FromMilliseconds(1));
        await using var restored = await StartAsync(clock, true, cancellationToken);
        await using var reader = await restored.ConnectAsync(cancellationToken);
        await RespCacheTests.ExchangeAsync(reader, RespTestHost.Command("GET", "x") + RespTestHost.Command("PTTL", "x"), "$3\r\nnew\r\n:3099\r\n", cancellationToken);
    }

    /// <summary>Condition-specific native authorization hooks are respected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task ConditionalWritesUseNativeAuthorization(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var host = await RespTestHost.StartAsync(cancellationToken, jwt: credentials, register: static services =>
        {
            _ = services.AddSingleton<IBinaryAuthorizationHook>(new DenyConditionalWrites());
        });
        await using var stream = await host.ConnectAsync(cancellationToken);
        var commands = RespTestHost.Command("AUTH", TestJwtHelper.CreateBearerToken(credentials)) + RespTestHost.Command("SET", "live", "old") +
                       RespTestHost.Command("SETEX", "literalNx", "30", "NX") + RespTestHost.Command("PSETEX", "literalXx", "30000", "XX") +
                       RespTestHost.Command("SETNX", "miss", "no") + RespTestHost.Command("SET", "miss", "no", "PX", "300", "NX") +
                       RespTestHost.Command("SET", "live", "no", "XX") + RespTestHost.Command("GET", "live") + RespTestHost.Command("GET", "miss");
        const string expected = "+OK\r\n+OK\r\n+OK\r\n+OK\r\n-NOPERM cache operation denied\r\n-NOPERM cache operation denied\r\n-NOPERM cache operation denied\r\n$3\r\nold\r\n$-1\r\n";
        await RespCacheTests.ExchangeAsync(stream, commands, expected, cancellationToken);
    }

    /// <summary>Conditional replacement can change expiry and requires permission to write the full entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task UpdatePermissionAloneCannotReplaceEntry(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var host = await RespTestHost.StartAsync(cancellationToken, jwt: credentials, register: static services =>
        {
            _ = services.AddSingleton<IBinaryAuthorizationHook>(new DenyFullEntryWrites());
        });
        await using var stream = await host.ConnectAsync(cancellationToken);
        var setup = RespTestHost.Command("AUTH", TestJwtHelper.CreateBearerToken(credentials)) +
                    RespTestHost.Command("SET", "live", "old", "NX", "EX", "60");
        await RespCacheTests.ExchangeAsync(stream, setup, "+OK\r\n+OK\r\n", cancellationToken);

        var extend = await WriteAndReadLineAsync(stream, RespTestHost.Command("SET", "live", "new", "XX", "EX", "120"), cancellationToken);
        _ = await Assert.That(extend).IsEqualTo("-NOPERM cache operation denied");
        var clear = await WriteAndReadLineAsync(stream, RespTestHost.Command("SET", "live", "new", "XX"), cancellationToken);
        _ = await Assert.That(clear).IsEqualTo("-NOPERM cache operation denied");

        var read = RespTestHost.Command("GET", "live") + RespTestHost.Command("PERSIST", "live");
        await RespCacheTests.ExchangeAsync(stream, read, "$3\r\nold\r\n:1\r\n", cancellationToken);
    }

    private static async Task<string?> WriteAndReadLineAsync(SslStream stream, string command, CancellationToken cancellationToken)
    {
        await stream.WriteAsync(Encoding.UTF8.GetBytes(command), cancellationToken);
        using var reader = new StreamReader(stream, Encoding.ASCII, false, 128, true);
        return await reader.ReadLineAsync(cancellationToken);
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

    private sealed class DenyFullEntryWrites : IBinaryAuthorizationHook
    {
        public ValueTask<FrameStatus> AuthorizeAsync(BinaryRequestContext request) =>
            ValueTask.FromResult(request.Path.EndsWith("/SetEntry", StringComparison.Ordinal) ? FrameStatus.PermissionDenied : FrameStatus.Ok);
    }

    private sealed class DenyConditionalWrites : IBinaryAuthorizationHook
    {
        public ValueTask<FrameStatus> AuthorizeAsync(BinaryRequestContext request)
        {
            var denied = request.Path.EndsWith("/TryAddEntry", StringComparison.Ordinal) || request.Path.EndsWith("/Update", StringComparison.Ordinal);
            return ValueTask.FromResult(denied ? FrameStatus.PermissionDenied : FrameStatus.Ok);
        }
    }
}
