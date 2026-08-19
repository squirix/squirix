using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Construction and validation rules of the static group composition.</summary>
[Immutable]
public sealed class GroupCompositionTests : ServerUnitTestBase
{
    /// <summary>A composition must not accept the same group twice.</summary>
    [Fact]
    public void CreateRejectsDuplicateGroupId() => Assert.Throws<ArgumentException>(static () => GroupComposition.Create("grp-1", "grp-1"));

    /// <summary>Distinct group identifiers form a valid composition.</summary>
    [Fact]
    public void CreateAcceptsDistinctGroupIds()
    {
        var composition = GroupComposition.Create("grp-1", "grp-2");

        Assert.True(composition.Contains("grp-1"));
        Assert.True(composition.Contains("grp-2"));
    }
}
