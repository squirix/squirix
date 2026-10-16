using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>What the slot rule needs to know about the group whose two snapshot slots it judges.</summary>
/// <param name="GroupId">The group the snapshot must belong to.</param>
/// <param name="MaxSnapshotBytes">The maximum accepted image size in bytes.</param>
/// <param name="FirstPath">The path of slot 0, named in failures about that slot; failures about both slots name its directory.</param>
/// <param name="SecondPath">The path of slot 1, named in failures about that slot.</param>
[Immutable]
internal readonly record struct GroupSnapshotSlotContext(string GroupId, int MaxSnapshotBytes, string FirstPath, string SecondPath);
