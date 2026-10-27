using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.Replication;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Reopens the groups of a data directory with the member state of one group copy rewritten, as a copy left in that state by an earlier run.</summary>
internal static class PlantedCopyTestKit
{
    /// <summary>Rewrites the member state in the metadata file of a group and opens a registry over the directory without seeding any group.</summary>
    /// <param name="dir">Node data directory whose groups were seeded and closed.</param>
    /// <param name="groupIds">The served group identifiers.</param>
    /// <param name="groupId">The group whose copy gets <paramref name="copy" />.</param>
    /// <param name="copy">The member state to plant and whether the planted record holds an offer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The opened registry.</returns>
    internal static async Task<ReplicaGroupRegistry> ReopenWithStateAsync(string dir, string[] groupIds, string groupId, (GroupMemberState State, bool Offered) copy, CancellationToken cancellationToken)
    {
        var path = GroupStoragePaths.GetMetadataPath(dir, groupId);
        var planted = await GroupMetadataFile.ReadAsync(path, cancellationToken) with { MemberState = copy.State, BootstrapOffered = copy.Offered };
        await GroupMetadataFile.WriteAsync(path, in planted, cancellationToken);
        var options = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        var registry = new ReplicaGroupRegistry(dir, groupIds, 3, ReplicaOwnerTestKit.Fingerprint, 1, NullLoggerFactory.Instance, options) { ElectionClock = TimeProvider.System };
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }
}
