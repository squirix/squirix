using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Replication;

/// <summary>Durable bootstrap state for one group.</summary>
/// <param name="GroupId">Replica group identity.</param>
/// <param name="State">Latest completed bootstrap stage.</param>
[Immutable]
internal sealed record BootstrapGroupProgress(string GroupId, BootstrapGroupState State);
