using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Node n1 serves groups n1, n2 and n3 and leads n1 and n2: a write commits in the led group that owns its key, a key of a group it only
/// follows is refused as a stale owner, and the committers are disposed together.
/// </summary>
public sealed class ReplicaGroupCommittersTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    /// <summary>Each write commits in the log of the led group that owns its key, and nothing reaches the other logs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteCommitsInOwnerGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-dispatch");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var cache = new StubCache();
        await using var committers = LeadTwo(registry, (new ScriptedGateway(), new ScriptedGateway()), cache, TimeProvider.System);
        var replicated = new ReplicatedCache(cache, committers);

        await replicated.SetEntryAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        await replicated.SetEntryAsync(NewOperationId(), CacheName, "a", Entry("a"), cancellationToken);

        await SequenceAssert.EqualAsync(["a"], await KeysAsync(registry, "n1", cancellationToken), StringComparer.Ordinal);
        await SequenceAssert.EqualAsync(["b"], await KeysAsync(registry, "n2", cancellationToken), StringComparer.Ordinal);
        _ = await Assert.That((await KeysAsync(registry, "n3", cancellationToken)).Count).IsEqualTo(0);
    }

    /// <summary>A key of a group the node only follows is refused as a stale owner, and nothing is appended to any log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowedGroupKeyIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-refused");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var gateway = new ScriptedGateway();
        await using var committers = LeadTwo(registry, (gateway, gateway), new StubCache(), TimeProvider.System);

        var failure = NodeExceptionAssert.For<RpcException>().Throws(committers, static led => _ = led.ForKey(CacheName, "c"));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(failure.Status.Detail).IsEqualTo("Key is owned by 'n3', not current node 'n1'.");
        foreach (var group in Groups)
            _ = await Assert.That((await KeysAsync(registry, group, cancellationToken)).Count).IsEqualTo(0);
        _ = await Assert.That(gateway.Appends.Count).IsEqualTo(0);
    }

    /// <summary>A node that leads only its own group takes every key to its committer without asking for the owner.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnGroupSkipsOwnerLookup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-own");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await using var own = CreateGroupCommitter(registry, "n1", new ScriptedGateway(), new StubCache(), TimeProvider.System);

        // The owner lookup has no setups: asking it would fail the test.
        await using var committers = new ReplicaGroupCommitters([own], "n1", new INodeLocatorCreateExpectations().Instance(), TimeProvider.System);

        _ = await Assert.That(committers.ForKey(CacheName, "c")).IsSameReferenceAs(own);
        _ = await Assert.That((committers.Leads("n1"), committers.Leads("n2"))).IsEqualTo((true, false));
    }

    /// <summary>Disposing the committers disposes every led committer, so each refuses a write afterwards.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDisposesEveryGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-led-groups-dispose");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var committers = LeadTwo(registry, (new ScriptedGateway(), new ScriptedGateway()), new StubCache(), TimeProvider.System);
        await committers.For("n1").CommitSetAsync(NewOperationId(), CacheName, "a", Entry("a"), cancellationToken);
        await committers.For("n2").CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);

        await committers.DisposeAsync();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(committers.For("n1").CommitSetAsync(NewOperationId(), CacheName, "a", Entry("a"), cancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(committers.For("n2").CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
    }
}
