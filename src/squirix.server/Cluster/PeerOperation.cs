namespace Squirix.Server.Cluster;

/// <summary>The operation code in a native peer request header.</summary>
internal enum PeerOperation : byte
{
    /// <summary>Checks whether the peer session is responsive.</summary>
    Ping = 0,

    /// <summary>Stores a cache entry on its owner.</summary>
    SetEntry = 1,

    /// <summary>Reads a cache entry on its owner.</summary>
    GetEntry = 2,

    /// <summary>Reads a cache value on its owner.</summary>
    GetValue = 3,

    /// <summary>Removes an entry on its owner.</summary>
    Remove = 4,

    /// <summary>Adds an entry if absent on its owner.</summary>
    TryAddEntry = 5,

    /// <summary>Changes an entry deadline on its owner.</summary>
    Touch = 6,

    /// <summary>Removes an entry deadline on its owner.</summary>
    RemoveExpiration = 7,

    /// <summary>Updates an existing entry on its owner.</summary>
    Update = 8,

    /// <summary>Reads an entry deadline on its owner.</summary>
    GetExpiration = 9,

    /// <summary>Reads or adds an entry on its owner.</summary>
    GetOrAdd = 10,

    /// <summary>Stores a distributed cache value on its owner.</summary>
    SetDistributed = 11,

    /// <summary>Reads a distributed cache value on its owner.</summary>
    GetDistributed = 12,

    /// <summary>Refreshes a distributed cache value on its owner.</summary>
    RefreshDistributed = 13,

    /// <summary>Replaces an existing entry, including its deadline, on its owner.</summary>
    ReplaceEntryIfPresent = 14,

    /// <summary>Reads a value on its owner with the RESP bulk string size limit.</summary>
    GetRespValue = 15,

    /// <summary>Appends entries to the replica log.</summary>
    AppendReplicaEntries = 16,

    /// <summary>Reads replica status.</summary>
    GetReplicaStatus = 17,

    /// <summary>Offers a bootstrap source to a replica.</summary>
    OfferReplicaBootstrap = 18,

    /// <summary>Checks whether a replica would vote.</summary>
    PreVote = 19,

    /// <summary>Requests a replica vote.</summary>
    RequestVote = 20,

    /// <summary>Transfers a part of the replica state.</summary>
    TransferReplicaState = 21,

    /// <summary>Invites one successor to campaign after retirement.</summary>
    TransferLeadership = 22,

    /// <summary>Requests a pre-vote on behalf of a retired leader.</summary>
    TransferPreVote = 23,
}
