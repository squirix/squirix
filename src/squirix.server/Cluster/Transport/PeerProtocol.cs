using Squirix.Transport.Wire.Cache;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Maps native peer operations to their routes and diagnostic paths.</summary>
internal static class PeerProtocol
{
    internal const string Prefix = "/squirix/peer/v1/";

    internal static (string? Path, BinaryOperation? OwnerOperation) Route(PeerOperation operation) => operation switch
    {
        PeerOperation.SetEntry => (Prefix + "owner/" + nameof(PeerOperation.SetEntry), BinaryOperation.SetEntry),
        PeerOperation.GetEntry => (Prefix + "owner/" + nameof(PeerOperation.GetEntry), BinaryOperation.GetEntry),
        PeerOperation.GetValue => (Prefix + "owner/" + nameof(PeerOperation.GetValue), BinaryOperation.GetValue),
        PeerOperation.Remove => (Prefix + "owner/" + nameof(PeerOperation.Remove), BinaryOperation.Remove),
        PeerOperation.TryAddEntry => (Prefix + "owner/" + nameof(PeerOperation.TryAddEntry), BinaryOperation.TryAddEntry),
        PeerOperation.Touch => (Prefix + "owner/" + nameof(PeerOperation.Touch), BinaryOperation.Touch),
        PeerOperation.RemoveExpiration => (Prefix + "owner/" + nameof(PeerOperation.RemoveExpiration), BinaryOperation.RemoveExpiration),
        PeerOperation.Update => (Prefix + "owner/" + nameof(PeerOperation.Update), BinaryOperation.Update),
        PeerOperation.GetExpiration => (Prefix + "owner/" + nameof(PeerOperation.GetExpiration), BinaryOperation.GetExpiration),
        PeerOperation.GetOrAdd => (Prefix + "owner/" + nameof(PeerOperation.GetOrAdd), BinaryOperation.GetOrAdd),
        PeerOperation.SetDistributed => (Prefix + "owner/" + nameof(PeerOperation.SetDistributed), BinaryOperation.SetDistributed),
        PeerOperation.GetDistributed => (Prefix + "owner/" + nameof(PeerOperation.GetDistributed), BinaryOperation.GetDistributed),
        PeerOperation.RefreshDistributed => (Prefix + "owner/" + nameof(PeerOperation.RefreshDistributed), BinaryOperation.RefreshDistributed),
        PeerOperation.AppendReplicaEntries => (Prefix + "replication/" + nameof(PeerOperation.AppendReplicaEntries), null),
        PeerOperation.GetReplicaStatus => (Prefix + "replication/" + nameof(PeerOperation.GetReplicaStatus), null),
        PeerOperation.OfferReplicaBootstrap => (Prefix + "replication/" + nameof(PeerOperation.OfferReplicaBootstrap), null),
        PeerOperation.TransferLeadership => (Prefix + "replication/" + nameof(PeerOperation.TransferLeadership), null),
        PeerOperation.TransferPreVote => (Prefix + "replication/" + nameof(PeerOperation.TransferPreVote), null),
        PeerOperation.PreVote => (Prefix + "replication/" + nameof(PeerOperation.PreVote), null),
        PeerOperation.RequestVote => (Prefix + "replication/" + nameof(PeerOperation.RequestVote), null),
        PeerOperation.TransferReplicaState => (Prefix + "replication/" + nameof(PeerOperation.TransferReplicaState), null),
        _ => default,
    };
}
