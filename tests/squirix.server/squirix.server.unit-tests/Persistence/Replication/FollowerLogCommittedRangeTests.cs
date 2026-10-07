using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>The bounded read of committed entries above an index returns the dense committed run, and nothing above the commit index.</summary>
public sealed class FollowerLogCommittedRangeTests : ServerUnitTestBase
{
    private const string GroupId = "grp-committed-range";

    /// <summary>The range stops at the commit index and at the requested count, and starts right above the given index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedRangeStopsAtCommitAndCount(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-committed-range");
        await using var log = await SeedAsync(dir, 5UL, 4UL, cancellationToken);

        var capped = await log.GetCommittedEntriesAsync(1UL, 2, cancellationToken);
        var toCommit = await log.GetCommittedEntriesAsync(1UL, 100, cancellationToken);

        _ = await Assert.That(Describe(capped)).IsEqualTo("2:p2,3:p3");
        _ = await Assert.That(Describe(toCommit)).IsEqualTo("2:p2,3:p3,4:p4");
    }

    /// <summary>An index at or above the commit index yields nothing, even when uncommitted entries follow it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedRangeEmptyAboveCommit(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-committed-range-empty");
        await using var log = await SeedAsync(dir, 5UL, 4UL, cancellationToken);

        var atCommit = await log.GetCommittedEntriesAsync(4UL, 10, cancellationToken);
        var aboveCommit = await log.GetCommittedEntriesAsync(5UL, 10, cancellationToken);
        var none = await log.GetCommittedEntriesAsync(0UL, 0, cancellationToken);

        _ = await Assert.That(atCommit).IsEmpty();
        _ = await Assert.That(aboveCommit).IsEmpty();
        _ = await Assert.That(none).IsEmpty();
    }

    private static string Describe(IReadOnlyList<FollowerLogEntry> entries)
    {
        var parts = new string[entries.Count];
        for (var index = 0; index < parts.Length; index++)
            parts[index] = entries[index].LogIndex + ":" + Encoding.UTF8.GetString(entries[index].Payload.Span);

        return string.Join(',', parts);
    }

    private static async Task<FollowerLog> SeedAsync(string dir, ulong last, ulong commit, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        try
        {
            await log.OpenAsync(cancellationToken);
            for (var index = 1UL; index <= last; index++)
                _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", index, 1UL, "p" + index), cancellationToken);

            _ = await log.AdvanceCommitAsync(commit, cancellationToken);
            return log;
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }
    }
}
