using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Holds the append batches with entries that one group sends to one follower until the test releases them, while empty probes and heartbeats
/// pass through, so the follower stays up and counts as ready but does not acknowledge the entries.
/// </summary>
internal sealed class ParkedAppendGateway : IReplicaRpcGateway
{
    private readonly string _followerNodeId;
    private readonly string _groupId;
    private readonly IReplicaRpcGateway _inner;
    private readonly TaskCompletionSource _release;

    private ParkedAppendGateway(IReplicaRpcGateway inner, string groupId, string followerNodeId, TaskCompletionSource release)
    {
        _inner = inner;
        _groupId = groupId;
        _followerNodeId = followerNodeId;
        _release = release;
    }

    public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
    {
        if (batch.Records.Count > 0 && string.Equals(nodeId, _followerNodeId, StringComparison.Ordinal) && string.Equals(header.GroupId, _groupId, StringComparison.Ordinal))
            await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

        return await _inner.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Builds a services callback that wraps the replica gateway of a node so it parks the entries of <paramref name="groupId" /> sent to <paramref name="followerNodeId" />.</summary>
    /// <param name="groupId">The group whose entries are parked.</param>
    /// <param name="followerNodeId">The follower the entries are sent to.</param>
    /// <param name="release">Completed by the test to let the parked batches through.</param>
    /// <returns>The services callback.</returns>
    internal static Action<IServiceCollection> CreateConfigure(string groupId, string followerNodeId, TaskCompletionSource release)
    {
        ArgumentNullException.ThrowIfNull(release);
        return services =>
        {
            for (var i = services.Count - 1; i >= 0; i--)
            {
                if (services[i].ServiceType == typeof(IReplicaRpcGateway))
                    services.RemoveAt(i);
            }

            _ = services.AddSingleton<IReplicaRpcGateway>(sp => new ParkedAppendGateway(sp.GetRequiredService<ReplicaRpcGateway>(), groupId, followerNodeId, release));
        };
    }
}
