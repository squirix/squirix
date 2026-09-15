using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Integration coverage for on-disk journal quota hard-limit rejection (issue #164).</summary>
public sealed class JournalDiskQuotaIntegrationTests : NodeIntegrationTestBase
{
    /// <summary>
    /// Fills a 1 MiB journal cap until durable appends are rejected without crashing the node,
    /// and verifies readiness plus <c language="csharp">journalDisk</c> pressure details remain available.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteAtCapFailsReadyStaysHealthy(CancellationToken cancellationToken)
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
            },
            cancellationToken);

        var journal = node.Services.GetRequiredService<IJournalCoordinator>();
        _ = await Assert.That(journal.MaxBytes).IsEqualTo(1024L * 1024L);

        var rejection = await FillUntilJournalQuotaAsync(journal, cancellationToken);
        _ = await Assert.That(rejection is JournalCapacityExceededException).IsTrue();

        using (var live = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), cancellationToken))
            _ = live.EnsureSuccessStatusCode();

        await AssertJournalDiskPressureAsync(uri, cancellationToken);

        // Node remains usable for another capacity-miss after the first rejection (pipeline not failed).
        var cacheKey = new CacheKey(ServerCacheNames.DefaultNamespace, "quota:again");
        var second = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(
            journal.AppendPutAndAwaitDurabilityAsync(cacheKey, new byte[200 * 1024], cancellationToken));
        _ = await Assert.That(second).IsNotNull();
    }

    private static async Task<Exception> FillUntilJournalQuotaAsync(IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        var bytes = new byte[200 * 1024];
        for (var i = 0; i < 32; i++)
        {
            try
            {
                await journal.AppendPutAndAwaitDurabilityAsync(new CacheKey(ServerCacheNames.DefaultNamespace, $"quota:k{i}"), bytes, cancellationToken).ConfigureAwait(false);
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

    private async Task AssertJournalDiskPressureAsync(Uri uri, CancellationToken cancellationToken)
    {
        var text = await HttpClient.GetStringAsync(new Uri(uri, "/health/ready/details"), cancellationToken).ConfigureAwait(false);
        using var document = JsonDocument.Parse(text);
        var details = document.RootElement.Clone();
        _ = await Assert.That(details.TryGetProperty("journalDisk", out var journalDisk)).IsTrue();
        var state = journalDisk.GetProperty("state").GetString();
        _ = await Assert.That(string.Equals(state, "high", StringComparison.Ordinal) || string.Equals(state, "critical", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(journalDisk.GetProperty("usedBytes").GetInt64() >= journalDisk.GetProperty("highWaterBytes").GetInt64()).IsTrue();
    }
}
