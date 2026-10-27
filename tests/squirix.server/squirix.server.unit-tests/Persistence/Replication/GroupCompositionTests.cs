using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Construction and validation rules of the static group composition.</summary>
[Immutable]
public sealed class GroupCompositionTests : ServerUnitTestBase
{
    /// <summary>Distinct group identifiers form a valid composition.</summary>
    [Test]
    public async Task CreateAcceptsDistinctGroupIds()
    {
        var composition = GroupComposition.Create("grp-1", "grp-2");

        _ = await Assert.That(composition.Contains("grp-1")).IsTrue();
        _ = await Assert.That(composition.Contains("grp-2")).IsTrue();
    }

    /// <summary>A composition must not accept the same group twice.</summary>
    [Test]
    public void CreateRejectsDuplicateGroupId() => _ = NodeExceptionAssert.For<ArgumentException>().Throws(static () => GroupComposition.Create("grp-1", "grp-1"));
}
