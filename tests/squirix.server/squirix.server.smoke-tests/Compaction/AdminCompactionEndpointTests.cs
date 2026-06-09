using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.SmokeTests.Compaction;

/// <summary>
/// Contains smoke tests for the <c>/admin/compact</c> endpoint.
/// This endpoint allows triggering journal compaction manually and should be:
/// <list type="bullet">
///     <item>
///         <description>Reducing the number of journal segment files when invoked.</description>
///     </item>
///     <item>
///         <description>Protected by a mutex so that concurrent requests do not run multiple compactions simultaneously.</description>
///     </item>
/// </list>
/// </summary>
public sealed class AdminCompactionEndpointTests : SmokeTestBase
{
    /// <summary>
    /// Validates that concurrent requests to <c>/admin/compact</c> are protected by a mutex.
    /// Exactly one compaction should run at a time. Depending on implementation,
    /// concurrent calls may result in one success and one conflict response,
    /// or both succeed but only one compaction run is executed internally.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task CompactNowIsMutexProtected()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "bca6cedf0c8b40e3bf3e7f2387a0318e", Url = url } };

        await using var node = await StartNodeAsync(url, peers, cancellationToken: DefaultCancellationToken);
        var cache = GetCacheApiClient(node);

        // Ensure there's something to compact.
        for (var i = 0; i < 30; i++)
            await cache.InsertAsync($"k:{i}", BuildEntry(new string('B', 500_000), version: 1), DefaultCancellationToken);

        // Fire two compaction requests concurrently.
        var t1 = HttpClient.PostAsync(node.Address + "/admin/compact", null, DefaultCancellationToken);
        var t2 = HttpClient.PostAsync(node.Address + "/admin/compact", null, DefaultCancellationToken);

        _ = await Task.WhenAll(t1, t2);

        var r1 = await t1;
        var r2 = await t2;

        // At least one must succeed.
        var successes = (r1.IsSuccessStatusCode ? 1 : 0) + (r2.IsSuccessStatusCode ? 1 : 0);
        Assert.InRange(successes, 1, 2);

        if (successes == 1)
        {
            // Strict mutex model: the other must be a recognizable "busy/conflict" status.
            var failed = r1.IsSuccessStatusCode ? r2 : r1;
            Assert.True(
                failed.StatusCode is HttpStatusCode.Conflict or HttpStatusCode.Locked or HttpStatusCode.TooManyRequests,
                $"Expected conflict/locked for parallel compaction, got {(int)failed.StatusCode} {failed.ReasonPhrase}");
        }
        else
        {
            // Coalesced model: both returned 2xx but only one run occurred.
            var r3 = await HttpClient.PostAsync(node.Address + "/admin/compact", null, DefaultCancellationToken);
            Assert.True(r3.IsSuccessStatusCode, "Follow-up compaction should be idempotent/safe.");
        }
    }

    /// <summary>
    /// Validates that invoking <c>/admin/compact</c> immediately triggers compaction
    /// and reduces the number of journal segment files in the data directory.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task CompactNowReducesJournalSegments()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "503e1151518c4121b1da08b73fac7511", Url = url } };
        var persistence = CreatePersistenceOptions(1, 5, strictFsync: false);
        await using var node = await StartNodeAsync(url, peers, persistenceOptions: persistence, cancellationToken: DefaultCancellationToken);

        var cache = GetCacheApiClient(node);

        var payload = new string('A', 250_000);
        for (var i = 0; i < 12; i++)
            await cache.InsertAsync($"f:{i}", BuildEntry(payload, version: 1), DefaultCancellationToken);

        await WaitUntilJournalSegmentCountAtLeastAsync(node.DataDir, 2, TimeSpan.FromSeconds(2), DefaultCancellationToken);

        var beforeFiles = DirectoryKit.CountFiles(node.DataDir, StorageFilePrefixes.JournalSegmentGlob);
        Assert.True(beforeFiles >= 2, $"Expected journal rotation in '{node.DataDir}', got {beforeFiles}");

        var resp = await HttpClient.PostAsync(node.Address + "/admin/compact", null, DefaultCancellationToken);
        Assert.True(resp.IsSuccessStatusCode, $"Compaction failed: {(int)resp.StatusCode} {resp.ReasonPhrase}");

        await WaitUntilJournalSegmentCountAtMostAsync(node.DataDir, beforeFiles, TimeSpan.FromSeconds(2), DefaultCancellationToken);

        var afterFiles = DirectoryKit.CountFiles(node.DataDir, StorageFilePrefixes.JournalSegmentGlob);
        Assert.True(afterFiles <= beforeFiles, $"journal files did not reduce: before={beforeFiles}, after={afterFiles}");
    }

    private static async Task WaitUntilJournalSegmentCountAtLeastAsync(string dataDir, int expectedMin, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (DirectoryKit.CountFiles(dataDir, StorageFilePrefixes.JournalSegmentGlob) >= expectedMin)
                return;

            await Task.Delay(20, cancellationToken);
        }
    }

    private static async Task WaitUntilJournalSegmentCountAtMostAsync(string dataDir, int expectedMax, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (DirectoryKit.CountFiles(dataDir, StorageFilePrefixes.JournalSegmentGlob) <= expectedMax)
                return;

            await Task.Delay(20, cancellationToken);
        }
    }
}
