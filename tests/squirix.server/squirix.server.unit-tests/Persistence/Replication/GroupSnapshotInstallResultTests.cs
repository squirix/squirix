using Squirix.Server.Storage.Replication;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>
/// Pins the default-instance safety of <see cref="GroupSnapshotInstallResult" />: a struct default bypasses
/// both factories, so the refusal marker must be nullable and normalize to empty for consumers.
/// </summary>
public sealed class GroupSnapshotInstallResultTests
{
    /// <summary>A default instance reports no success and an empty normalized refusal marker.</summary>
    [Fact]
    public void DefaultIsNotSuccessfulWithEmptyRefusal()
    {
        var result = default(GroupSnapshotInstallResult);

        Assert.False(result.Success);
        Assert.Equal(string.Empty, result.Refusal);
    }

    /// <summary>The accepted outcome reports success and carries an empty refusal marker.</summary>
    [Fact]
    public void InstalledCarriesEmptyRefusal()
    {
        var result = GroupSnapshotInstallResult.Installed;

        Assert.True(result.Success);
        Assert.Equal(string.Empty, result.Refusal);
    }

    /// <summary>The refused outcome reports failure and keeps its stable marker through the normalized accessor.</summary>
    [Fact]
    public void RefusedKeepsMarkerThroughRefusal()
    {
        var result = GroupSnapshotInstallResult.Refused("not-ready");

        Assert.False(result.Success);
        Assert.Equal("not-ready", result.Refusal);
    }
}
