using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Maps native peer operations to their routes and diagnostic paths.</summary>
internal static class PeerProtocol
{
    internal const string Prefix = "/squirix/peer/v1/";

    private const string OwnerPrefix = Prefix + "owner/";
    private const string ReplicationPrefix = Prefix + "replication/";

    internal static (string? Path, BinaryOperation? OwnerOperation) Route(PeerOperation operation) => operation switch
    {
        PeerOperation.SetEntry => (OwnerPrefix + nameof(PeerOperation.SetEntry), BinaryOperation.SetEntry),
        PeerOperation.GetEntry => (OwnerPrefix + nameof(PeerOperation.GetEntry), BinaryOperation.GetEntry),
        PeerOperation.GetValue => (OwnerPrefix + nameof(PeerOperation.GetValue), BinaryOperation.GetValue),
        PeerOperation.Remove => (OwnerPrefix + nameof(PeerOperation.Remove), BinaryOperation.Remove),
        PeerOperation.TryAddEntry => (OwnerPrefix + nameof(PeerOperation.TryAddEntry), BinaryOperation.TryAddEntry),
        PeerOperation.Touch => (OwnerPrefix + nameof(PeerOperation.Touch), BinaryOperation.Touch),
        PeerOperation.RemoveExpiration => (OwnerPrefix + nameof(PeerOperation.RemoveExpiration), BinaryOperation.RemoveExpiration),
        PeerOperation.Update => (OwnerPrefix + nameof(PeerOperation.Update), BinaryOperation.Update),
        PeerOperation.GetExpiration => (OwnerPrefix + nameof(PeerOperation.GetExpiration), BinaryOperation.GetExpiration),
        PeerOperation.GetOrAdd => (OwnerPrefix + nameof(PeerOperation.GetOrAdd), BinaryOperation.GetOrAdd),
        PeerOperation.SetDistributed => (OwnerPrefix + nameof(PeerOperation.SetDistributed), BinaryOperation.SetDistributed),
        PeerOperation.GetDistributed => (OwnerPrefix + nameof(PeerOperation.GetDistributed), BinaryOperation.GetDistributed),
        PeerOperation.RefreshDistributed => (OwnerPrefix + nameof(PeerOperation.RefreshDistributed), BinaryOperation.RefreshDistributed),
        PeerOperation.AppendReplicaEntries => (ReplicationPrefix + nameof(PeerOperation.AppendReplicaEntries), null),
        PeerOperation.GetReplicaStatus => (ReplicationPrefix + nameof(PeerOperation.GetReplicaStatus), null),
        PeerOperation.OfferReplicaBootstrap => (ReplicationPrefix + nameof(PeerOperation.OfferReplicaBootstrap), null),
        PeerOperation.TransferLeadership => (ReplicationPrefix + nameof(PeerOperation.TransferLeadership), null),
        PeerOperation.TransferPreVote => (ReplicationPrefix + nameof(PeerOperation.TransferPreVote), null),
        PeerOperation.PreVote => (ReplicationPrefix + nameof(PeerOperation.PreVote), null),
        PeerOperation.RequestVote => (ReplicationPrefix + nameof(PeerOperation.RequestVote), null),
        PeerOperation.TransferReplicaState => (ReplicationPrefix + nameof(PeerOperation.TransferReplicaState), null),
        _ => default,
    };
}
