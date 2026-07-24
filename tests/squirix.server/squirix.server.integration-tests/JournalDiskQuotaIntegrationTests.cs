using System;
using System.Buffers;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>Integration coverage for on-disk journal quota hard-limit rejection (issue #164).</summary>
public sealed class JournalDiskQuotaIntegrationTests : NodeIntegrationTestBase
{
    /// <summary>
    /// Fills a 1 MiB journal cap until durable appends are rejected without crashing the node,
    /// and verifies readiness plus <c>journalDisk</c> pressure details remain available.
    /// </summary>
    [Fact]
    public async Task DurableWriteAtJournalCapFailsControlledAndReadyStaysHealthy()
    {
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(
            uri,
            "node_journal_quota",
            new NodeStartOptions
            {
                PersistenceOptions = new PersistenceOptions
                {
                    JournalMaxTotalBytesMb = 1,
                    JournalMaxSegmentMb = 1,
                },
            });

        var journal = node.Services.GetRequiredService<IJournalCoordinator>();
        Assert.Equal(1024L * 1024L, journal.MaxBytes);

        var rejection = await FillUntilJournalQuotaAsync(journal).ConfigureAwait(true);
        Assert.True(rejection is JournalCapacityExceededException, $"Unexpected rejection type: {rejection.GetType().FullName}");

        using (var live = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), DefaultCancellationToken))
            _ = live.EnsureSuccessStatusCode();

        await AssertJournalDiskPressureAsync(uri).ConfigureAwait(true);

        // Node remains usable for another capacity-miss after the first rejection (pipeline not failed).
        var second = await Assert.ThrowsAsync<JournalCapacityExceededException>(() =>
            journal.AppendPutAndAwaitDurabilityAsync(
                new CacheKey(ServerCacheNames.DefaultNamespace, "quota:again"),
                new byte[200 * 1024],
                DefaultCancellationToken).AsTask()).ConfigureAwait(true);
        Assert.NotNull(second);
    }

    private static async Task<Exception> FillUntilJournalQuotaAsync(IJournalCoordinator journal)
    {
        var payload = ArrayPool<byte>.Shared.Rent(200 * 1024);
        try
        {
            var bytes = payload.AsMemory(0, 200 * 1024);
            for (var i = 0; i < 32; i++)
            {
                try
                {
                    await journal.AppendPutAndAwaitDurabilityAsync(
                        new CacheKey(ServerCacheNames.DefaultNamespace, $"quota:k{i}"),
                        bytes,
                        DefaultCancellationToken).ConfigureAwait(false);
                }
                catch (JournalCapacityExceededException ex)
                {
                    return ex;
                }
                catch (InvalidOperationException ex) when (ex.InnerException is JournalCapacityExceededException capacity)
                {
                    return capacity;
                }
            }

            Assert.Fail($"Expected journal capacity rejection. used={journal.UsedBytes} max={journal.MaxBytes} high={journal.HighWaterBytes}");
            throw new InvalidOperationException("unreachable");
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payload);
        }
    }

    private async Task AssertJournalDiskPressureAsync(Uri uri)
    {
        var details = await HttpClient.GetFromJsonAsync<JsonElement>(new Uri(uri, "/health/ready/details"), DefaultCancellationToken).ConfigureAwait(false);
        Assert.True(details.TryGetProperty("journalDisk", out var journalDisk));
        var state = journalDisk.GetProperty("state").GetString();
        Assert.True(
            string.Equals(state, "high", StringComparison.Ordinal) || string.Equals(state, "critical", StringComparison.Ordinal),
            $"Expected high/critical journalDisk.state, got '{state}'.");
        Assert.True(journalDisk.GetProperty("usedBytes").GetInt64() >= journalDisk.GetProperty("highWaterBytes").GetInt64());
    }
}
