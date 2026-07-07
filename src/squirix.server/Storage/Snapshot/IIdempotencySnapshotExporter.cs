using System;
using System.Collections.Generic;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>Exports live idempotency records for inclusion in node snapshots.</summary>
internal interface IIdempotencySnapshotExporter
{
    /// <summary>Copies non-expired idempotency records into <paramref name="destination" />.</summary>
    /// <param name="destination">Mutable list cleared and populated with exported records.</param>
    /// <param name="utcNow">Snapshot cut timestamp used for retention filtering.</param>
    void ExportSnapshot(List<PersistedIdempotencyRecord> destination, DateTime utcNow);
}
