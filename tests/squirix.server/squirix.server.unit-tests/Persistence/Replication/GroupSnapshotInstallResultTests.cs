using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>
/// Pins the default-instance safety of <see cref="GroupSnapshotInstallResult" />: a struct default bypasses
/// both factories, so the refusal marker must be nullable and normalize to empty for consumers.
/// </summary>
public sealed class GroupSnapshotInstallResultTests
{
    /// <summary>A default instance reports no success and an empty normalized refusal marker.</summary>
    [Test]
    public async Task DefaultIsNotSuccessfulWithEmptyRefusal()
    {
        var result = default(GroupSnapshotInstallResult);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.Refusal).IsEqualTo(string.Empty);
    }

    /// <summary>The accepted outcome reports success and carries an empty refusal marker.</summary>
    [Test]
    public async Task InstalledCarriesEmptyRefusal()
    {
        var result = GroupSnapshotInstallResult.Installed;

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That(result.Refusal).IsEqualTo(string.Empty);
    }

    /// <summary>The refused outcome reports failure and keeps its stable marker through the normalized accessor.</summary>
    [Test]
    public async Task RefusedKeepsMarkerThroughRefusal()
    {
        var result = GroupSnapshotInstallResult.Refused("not-ready");

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.Refusal).IsEqualTo("not-ready");
    }
}
