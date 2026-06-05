using System.Collections.Generic;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Describes read-only snapshot metadata and a bounded key preview.
/// </summary>
/// <param name="EntryCount">The number of active entries.</param>
/// <param name="IdempotencyRecordCount">The number of idempotency records.</param>
/// <param name="Entries">A bounded entry preview.</param>
[JetBrains.Annotations.UsedImplicitly(JetBrains.Annotations.ImplicitUseTargetFlags.WithMembers)]
internal sealed record SnapshotDumpReport(int EntryCount, int IdempotencyRecordCount, IReadOnlyList<SnapshotEntryPreview> Entries);
