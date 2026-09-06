using System.Collections.Generic;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Prepared bootstrap manifest summary for stopped-node scenarios.</summary>
/// <param name="TargetReplicaCount">Seeded target replica count.</param>
/// <param name="TargetGeneration">Seeded target configuration generation.</param>
/// <param name="PendingGroups">Seeded groups as <c language="none">group-id:State</c> entries.</param>
/// <param name="Resumed">Whether preparation resumed an identical existing manifest.</param>
public sealed record OfflineBootstrapSummary(int TargetReplicaCount, ulong TargetGeneration, IReadOnlyList<string> PendingGroups, bool Resumed);
