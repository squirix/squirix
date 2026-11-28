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

/// <summary>Exercises reads across the old expiration while a conditional replacement is waiting for a journal flush.</summary>
public sealed class RespReplacementReadTests : IsolatedStorageTestBase
{
    /// <summary>Owner and RESP reads see the replacement and its deadline even when the old entry expires during fsync.</summary>
    /// <param name="extendExpiration">Whether the replacement sets a longer TTL instead of removing it.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    [Timeout(30_000)]
    public async Task OverlappingReadsSeeDurableReplacement(bool extendExpiration, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var enteredRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (var gated = await StallableJournal.CreateAsync(Dir, false, cancellationToken))
        {
            await using var host = await StartWithGatedJournalAsync(gated, clock, enteredRead, cancellationToken);
            await using var writer = await host.ConnectAsync(cancellationToken);
            await using var reader = await host.ConnectAsync(cancellationToken);
            await RespCacheTests.ExchangeAsync(writer, RespTestHost.Command("SET", "x", "old", "PX", "1000"), "+OK\r\n", cancellationToken);
            var native = host.Server.Services.GetRequiredService<RoutedCacheOperations<StoredValue>>();
            await RunReplacementAsync(gated, clock, native, enteredRead, (writer, reader), extendExpiration, cancellationToken);
            _ = await Assert.That(gated.Journal.AppendedOps).IsEqualTo(2);
        }

        await using var recovered = await RespTestHost.StartAsync(
            cancellationToken,
            configure: options => options.UsePersistence(Dir),
            register: services => { _ = services.AddSingleton<TimeProvider>(clock); });
        await using var restored = await recovered.ConnectAsync(cancellationToken);
        await CheckRespValueAndTtlAsync(restored, extendExpiration, cancellationToken);
    }

    private static async Task RunReplacementAsync(
        StallableJournal gated,
        FakeTimeProvider clock,
        RoutedCacheOperations<StoredValue> native,
        TaskCompletionSource enteredRead,
        (SslStream Writer, SslStream Reader) connections,
        bool extendExpiration,
        CancellationToken cancellationToken)
    {
        var replacement = extendExpiration
            ? RespTestHost.Command("SET", "x", "new", "XX", "PX", "5000")
            : RespTestHost.Command("SET", "x", "new", "XX");
        gated.Writer.Flush.Arm();
        var writing = RespCacheTests.ExchangeAsync(connections.Writer, replacement, "+OK\r\n", cancellationToken);
        try
        {
            await gated.Writer.Flush.Entered.WaitAsync(cancellationToken);
            clock.Advance(TimeSpan.FromSeconds(1));
            var ownerRead = native.Reads.GetValueAsync(new GetValueAsyncRequest { CacheName = "resp", Key = "x" }, cancellationToken).AsTask();
            var entryRead = native.Reads.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "resp", Key = "x" }, cancellationToken).AsTask();
            var deadlineRead = native.Reads.GetExpirationAsync(new GetExpirationAsyncRequest { CacheName = "resp", Key = "x" }, cancellationToken).AsTask();
            var distributedRead = native.Reads.GetDistributedAsync(new GetDistributedAsyncRequest { CacheName = "resp", Key = "x" }, cancellationToken).AsTask();
            var wireRead = CheckRespValueAndTtlAsync(connections.Reader, extendExpiration, cancellationToken);
            try
            {
                await enteredRead.Task.WaitAsync(cancellationToken);
                _ = await Assert.That(ownerRead.IsCompleted).IsFalse();
                _ = await Assert.That(entryRead.IsCompleted).IsFalse();
                _ = await Assert.That(deadlineRead.IsCompleted).IsFalse();
                _ = await Assert.That(distributedRead.IsCompleted).IsFalse();
            }
            finally
            {
                gated.Writer.Flush.Release();
                await wireRead;
            }

            var value = await ownerRead;
            _ = await Assert.That(value.Found).IsTrue();
            _ = await Assert.That(Encoding.ASCII.GetString(value.Value!.BinaryValue.Span)).IsEqualTo("new");
            var entry = await entryRead;
            _ = await Assert.That(Encoding.ASCII.GetString(entry.Entry!.Value.Value!.BinaryValue.Span)).IsEqualTo("new");
            var expiration = await deadlineRead;
            _ = await Assert.That(expiration.Found).IsTrue();
            _ = await Assert.That(expiration.HasExpiration).IsEqualTo(extendExpiration);
            if (extendExpiration)
                _ = await Assert.That(expiration.Remaining).IsEqualTo(TimeSpan.FromSeconds(4));
            var error = await NodeAsyncAssert.ThrowsAsync<ServerCallException, GetDistributedAsyncResponse>(new ValueTask<GetDistributedAsyncResponse>(distributedRead), TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            _ = await Assert.That(error.StatusCode).IsEqualTo(FrameStatus.DataLoss);
        }
        finally
        {
            gated.Writer.Flush.Release();
            await writing;
        }
    }

    private static async Task CheckRespValueAndTtlAsync(SslStream stream, bool extendExpiration, CancellationToken cancellationToken)
    {
        var commands = RespTestHost.Command("GET", "x") + RespTestHost.Command("PTTL", "x");
        await stream.WriteAsync(Encoding.ASCII.GetBytes(commands), cancellationToken);
        using var reader = new StreamReader(stream, Encoding.ASCII, false, 128, true);
        _ = await Assert.That(await reader.ReadLineAsync(cancellationToken)).IsEqualTo("$3");
        _ = await Assert.That(await reader.ReadLineAsync(cancellationToken)).IsEqualTo("new");
        _ = await Assert.That(await reader.ReadLineAsync(cancellationToken)).IsEqualTo(extendExpiration ? ":4000" : ":-1");
    }

    private ValueTask<RespTestHost> StartWithGatedJournalAsync(
        StallableJournal gated,
        TimeProvider clock,
        TaskCompletionSource enteredRead,
        CancellationToken cancellationToken) =>
        RespTestHost.StartAsync(
            cancellationToken,
            configure: options => options.UsePersistence(Path.Join(Dir, "host")),
            register: services =>
            {
                _ = services.AddSingleton(clock);
                _ = services.AddSingleton<IJournalCoordinator>(gated.Journal);
                _ = services.AddSingleton(new BinaryCallObserver(context =>
                {
                    if (context.Path.EndsWith("/GetValue", StringComparison.Ordinal))
                        _ = enteredRead.TrySetResult();
                }));
            });
}
