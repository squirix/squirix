using System;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Unit tests for the names of the member states in the replica status.</summary>
[Immutable]
public sealed class GroupMemberStateNamesTests : ServerUnitTestBase
{
    /// <summary>Every state maps to its lowercase name.</summary>
    /// <returns>A task that completes when the assertions are done.</returns>
    [Test]
    public async Task EveryStateHasItsName()
    {
        _ = await Assert.That(GroupMemberStateNames.Of(GroupMemberState.Unseeded)).IsEqualTo("unseeded");
        _ = await Assert.That(GroupMemberStateNames.Of(GroupMemberState.Complete)).IsEqualTo("complete");
        _ = await Assert.That(GroupMemberStateNames.Of(GroupMemberState.Incomplete)).IsEqualTo("incomplete");
    }

    /// <summary>A value that names no state is refused.</summary>
    /// <returns>A task that completes when the assertions are done.</returns>
    [Test]
    public async Task UnnamedStateIsRefused()
    {
        var ex = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => _ = GroupMemberStateNames.Of(GroupMemberState.Incomplete + 1));

        _ = await Assert.That(ex.ParamName).IsEqualTo("state");
    }
}
