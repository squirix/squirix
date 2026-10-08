using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Utils;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Harness of a node that leads several replica groups: node n1 serves groups n1, n2 and n3 with RF=3 and leads n1 from slot 0 and n2
/// from slot 2. Key a belongs to group n1, key b to n2, and every other key to n3.
/// </summary>
internal static class LedGroupsTestKit
{
    /// <summary>The replica groups node n1 serves.</summary>
    internal static readonly string[] Groups = ["n1", "n2", "n3"];

    /// <summary>Creates the committer of a group on node n1, which takes the slot a rotating locator gives it: slot 0 of n1, slot 2 of n2.</summary>
    /// <param name="registry">Replica group registry of node n1, serving the group.</param>
    /// <param name="groupId">The led group.</param>
    /// <param name="gateway">Follower transport double.</param>
    /// <param name="cache">Local cache pipeline.</param>
    /// <param name="clock">The clock of the decisions.</param>
    /// <returns>The committer.</returns>
    internal static ReplicaGroupCommitter CreateGroupCommitter(
        ReplicaGroupRegistry registry,
        string groupId,
        IReplicaRpcGateway gateway,
        ILogicalNamespacedCache<object?> cache,
        TimeProvider clock) =>
        new(registry, new RotatingLocator(), gateway, cache, (groupId, "n1"), new ReplicaTopologyStamp(ReplicaOwnerTestKit.Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Recovery = ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
            Clock = clock,
        };

    /// <summary>Reads the keys of the entries a group log holds, in log order.</summary>
    /// <param name="registry">The registry serving the group.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The keys.</returns>
    /// <exception cref="InvalidOperationException">The group log is not open, or an entry does not decode.</exception>
    internal static async Task<List<string>> KeysAsync(ReplicaGroupRegistry registry, string groupId, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(groupId, out var log))
            throw new InvalidOperationException($"The group log {groupId} is not open.");

        var read = await log.ReadEntriesAsync(1, 64, cancellationToken);
        var keys = new List<string>(read.Entries.Count);
        foreach (var entry in read.Entries)
        {
            var record = ReplicaLogCodec.Decode(entry.Payload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The log entry must decode."));
            keys.Add(Encoding.UTF8.GetString(record.KeyPayload.Span));
        }

        return keys;
    }

    /// <summary>Leads only the group of <paramref name="committer" />, which this node owns, so the key owner lookup is never asked.</summary>
    /// <param name="committer">The committer of the own group.</param>
    /// <returns>The committers of the led groups, on the clock of <paramref name="committer" />.</returns>
    internal static ReplicaGroupCommitters LeadOwn(ReplicaGroupCommitter committer) =>
        new([committer], committer.GroupId, new INodeLocatorCreateExpectations().Instance(), committer.Clock);

    /// <summary>Creates the committers of node n1 leading groups n1 and n2, each with its own follower transport.</summary>
    /// <param name="registry">The registry of node n1.</param>
    /// <param name="gateways">The follower transports of groups n1 and n2.</param>
    /// <param name="cache">The local cache both groups apply to.</param>
    /// <param name="clock">The clock of the decisions.</param>
    /// <returns>The committers, which own the disposal of both committers.</returns>
    internal static ReplicaGroupCommitters LeadTwo(
        ReplicaGroupRegistry registry,
        (IReplicaRpcGateway OfN1, IReplicaRpcGateway OfN2) gateways,
        ILogicalNamespacedCache<object?> cache,
        TimeProvider clock)
    {
        var led = new ReplicaGroupCommitter[2];
        led[0] = CreateGroupCommitter(registry, "n1", gateways.OfN1, cache, clock);
        led[1] = CreateGroupCommitter(registry, "n2", gateways.OfN2, cache, clock);
        return new ReplicaGroupCommitters(led, "n1", Owners(), clock);
    }

    /// <summary>Creates the key owner lookup: key a belongs to n1, key b to n2, and every other key to n3.</summary>
    /// <returns>The lookup.</returns>
    internal static INodeLocator Owners()
    {
        var owners = new INodeLocatorCreateExpectations();
        _ = owners.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Callback(static (_, key) => key switch
        {
            "a" => "n1",
            "b" => "n2",
            _ => "n3",
        });
        return owners.Instance();
    }

    /// <summary>Places the owner of a group in slot 0 and the next nodes of n1, n2, n3 after it, wrapping around.</summary>
    private sealed class RotatingLocator : IReplicaGroupLocator
    {
        public int ReplicaCount => 3;

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination)
        {
            var first = Array.IndexOf(Groups, originalOwnerNodeId);
            for (var i = 0; i < Groups.Length; i++)
                destination[i] = Groups[(first + i) % Groups.Length];
        }
    }
}
