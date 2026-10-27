using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>The group line of the offline doctor names the member state of the copy.</summary>
[Immutable]
public sealed class DoctorMemberStateTests : ServerUnitTestBase
{
    private const string FingerprintHex = "0A0B";

    /// <summary>The line names a complete copy, an incomplete copy, and an unseeded copy with or without an offer; none of them is a mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReportsMemberStateOfCopies(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-member-state");
        await WriteAsync(dir, "n1", GroupMemberState.Complete, false, cancellationToken);
        await WriteAsync(dir, "n2", GroupMemberState.Incomplete, false, cancellationToken);
        await WriteAsync(dir, "n3", GroupMemberState.Unseeded, false, cancellationToken);
        await WriteAsync(dir, "n4", GroupMemberState.Unseeded, true, cancellationToken);

        var (hasMismatch, lines) = await ReplicaDoctorReportBuilder.BuildAsync(FingerprintHex, 5, 2, ["n1", "n2", "n3", "n4"], dir, cancellationToken);

        var text = string.Join('\n', lines);
        _ = await Assert.That(hasMismatch).IsFalse();
        _ = await Assert.That(text).Contains("group 'n1': term 9 commit 10 applied 7 apply-lag 3 fingerprint match generation match member complete\n", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("group 'n2': term 9 commit 10 applied 7 apply-lag 3 fingerprint match generation match member incomplete\n", StringComparison.Ordinal);
        _ = await Assert.That(text).Contains("group 'n3': term 9 commit 10 applied 7 apply-lag 3 fingerprint match generation match member unseeded not-offered\n", StringComparison.Ordinal);
        _ = await Assert.That(text).EndsWith("group 'n4': term 9 commit 10 applied 7 apply-lag 3 fingerprint match generation match member unseeded offered", StringComparison.Ordinal);
    }

    private static Task WriteAsync(string dir, string groupId, GroupMemberState state, bool offered, CancellationToken cancellationToken)
    {
        var meta = new GroupLogMetadata(groupId, Convert.FromHexString(FingerprintHex), 5, 9, string.Empty, 10, 7, state, offered);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, groupId));
        return GroupMetadataFile.WriteAsync(GroupStoragePaths.GetMetadataPath(dir, groupId), in meta, cancellationToken);
    }
}
