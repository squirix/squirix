using System;
using System.IO;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies that a persistent RESP removal records the tombstone and reports the observed presence.</summary>
public sealed class RespRemovalDurabilityTests : IsolatedStorageTestBase
{
    /// <summary>Removed keys stay absent when the node reopens the journal with an earlier wall clock.</summary>
    /// <param name="command">The command that takes the removal path.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("DEL")]
    [Arguments("UNLINK")]
    [Arguments("PEXPIRE")]
    [Timeout(30_000)]
    public async Task RemovalSurvivesRecoveryAtEarlierClock(string command, CancellationToken cancellationToken)
    {
        var started = DateTimeOffset.UnixEpoch.AddHours(1);
        var clock = new FakeTimeProvider(started);
        await using (var gated = await StallableJournal.CreateAsync(Dir, false, cancellationToken))
        {
            await using var host = await StartWithJournalAsync(gated, clock, cancellationToken);
            await using var client = await host.ConnectAsync(cancellationToken);

            await RespCacheTests.ExchangeAsync(client, RemoveCommand(command, "absent"), ":0\r\n", cancellationToken);
            _ = await Assert.That(gated.Journal.AppendedOps).IsEqualTo(1);
            var seed = RespTestHost.Command("SET", "expired", "old", "PX", "1000") + RespTestHost.Command("SET", "live", "value");
            await RespCacheTests.ExchangeAsync(client, seed, "+OK\r\n+OK\r\n", cancellationToken);

            clock.Advance(TimeSpan.FromSeconds(1));
            await RespCacheTests.ExchangeAsync(client, RemoveCommand(command, "expired"), ":0\r\n", cancellationToken);
            _ = await Assert.That(gated.Journal.AppendedOps).IsEqualTo(4);
            await RespCacheTests.ExchangeAsync(client, RemoveCommand(command, "live"), ":1\r\n", cancellationToken);
            await RespCacheTests.ExchangeAsync(client, RemoveCommand(command, "live"), ":0\r\n", cancellationToken);
            _ = await Assert.That(gated.Journal.AppendedOps).IsEqualTo(6);
        }

        await using var recovered = await RespTestHost.StartAsync(
            cancellationToken,
            configure: options => options.UsePersistence(Dir),
            register: services => _ = services.AddSingleton<TimeProvider>(new FakeTimeProvider(started)));
        await using var restored = await recovered.ConnectAsync(cancellationToken);
        await RespCacheTests.ExchangeAsync(restored, RespTestHost.Command("GET", "expired"), "$-1\r\n", cancellationToken);
        await RespCacheTests.ExchangeAsync(restored, RespTestHost.Command("GET", "live"), "$-1\r\n", cancellationToken);
    }

    /// <summary>A deletion decided for a live key returns one even when its old deadline crosses during the flush.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(30_000)]
    public async Task LiveRemovalKeepsResultAcrossExpiry(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        await using var gated = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var host = await StartWithJournalAsync(gated, clock, cancellationToken);
        await using var client = await host.ConnectAsync(cancellationToken);
        await RespCacheTests.ExchangeAsync(client, RespTestHost.Command("SET", "live", "value", "PX", "1000"), "+OK\r\n", cancellationToken);

        await RemoveWhileFlushIsStalledAsync(gated, clock, client, cancellationToken);

        _ = await Assert.That(gated.Journal.AppendedOps).IsEqualTo(2);
    }

    private static async Task RemoveWhileFlushIsStalledAsync(
        StallableJournal gated, FakeTimeProvider clock, SslStream client, CancellationToken cancellationToken)
    {
        gated.Writer.Flush.Arm();
        var removal = RespCacheTests.ExchangeAsync(client, RespTestHost.Command("DEL", "live"), ":1\r\n", cancellationToken);
        try
        {
            await gated.Writer.Flush.Entered.WaitAsync(cancellationToken);
            clock.Advance(TimeSpan.FromSeconds(1));
        }
        finally
        {
            gated.Writer.Flush.Release();
            await removal;
        }
    }

    private static string RemoveCommand(string command, string key) =>
        string.Equals(command, "PEXPIRE", StringComparison.Ordinal) ? RespTestHost.Command(command, key, "0") : RespTestHost.Command(command, key);

    private ValueTask<RespTestHost> StartWithJournalAsync(StallableJournal gated, TimeProvider clock, CancellationToken cancellationToken) =>
        RespTestHost.StartAsync(
            cancellationToken,
            configure: options => options.UsePersistence(Path.Join(Dir, "host")),
            register: services =>
            {
                _ = services.AddSingleton(clock);
                _ = services.AddSingleton<IJournalCoordinator>(gated.Journal);
            });
}
