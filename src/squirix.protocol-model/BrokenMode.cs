namespace Squirix.ProtocolModel;

/// <summary>Broken-rule injection modes for negative explorer runs.</summary>
internal enum BrokenMode
{
    /// <summary>No broken rule; expect safety invariants to hold.</summary>
    None = 0,

    /// <summary>Grant a second vote in a term already voted in; the up-to-date log check still applies.</summary>
    Vote = 1,

    /// <summary>Allow commit of old-term entries without a current-term commit.</summary>
    CurrentTermCommit = 2,

    /// <summary>Mark read-index ready without majority confirm / apply wait.</summary>
    ReadIndex = 3,

    /// <summary>Repair a member that lost its disk without the term ceiling taken from a majority.</summary>
    RepairCeiling = 4,
}
