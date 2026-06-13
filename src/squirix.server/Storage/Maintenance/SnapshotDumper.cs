using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Provides read-only snapshot metadata dumps.
/// </summary>
[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal static class SnapshotDumper
{
    /// <summary>
    /// Loads snapshot metadata and a bounded key preview.
    /// </summary>
    /// <param name="path">Snapshot file path.</param>
    /// <param name="maxEntries">Maximum number of entry previews.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The dump report.</returns>
    public static async Task<SnapshotDumpReport> DumpAsync(string path, int maxEntries = 20, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentOutOfRangeException.ThrowIfNegative(maxEntries);

        var loaded = await SnapshotReader.LoadStrictAsync<object?>(path, true, cancellationToken).ConfigureAwait(false);
        var count = Math.Min(maxEntries, loaded.Entries.Count);
        var entries = new List<SnapshotEntryPreview>(count);
        for (var i = 0; i < count; i++)
        {
            var (key, entry) = loaded.Entries[i];
            entries.Add(new SnapshotEntryPreview(key.ToString(), entry.Version));
        }

        return new SnapshotDumpReport(loaded.Entries.Count, loaded.IdempotencyRecords.Count, entries);
    }
}
