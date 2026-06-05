namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Per-node ring distribution for admin diagnostics.
/// </summary>
internal readonly record struct AdminRingNodeDistributionSnapshot(string NodeId, int SampleKeys, double SampleShare, int ConfiguredVirtualNodes);
