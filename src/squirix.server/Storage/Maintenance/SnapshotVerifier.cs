using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Provides read-only verification of snapshot files.
/// </summary>
[JetBrains.Annotations.UsedImplicitly(JetBrains.Annotations.ImplicitUseTargetFlags.WithMembers)]
internal static class SnapshotVerifier
{
    /// <summary>
    /// Verifies snapshot framing and payloads without modifying storage.
    /// </summary>
    /// <param name="path">Snapshot file path.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The verification result.</returns>
    public static async Task<StorageVerificationResult> VerifyAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var full = Path.GetFullPath(path);
        if (!File.Exists(full))
            return new StorageVerificationResult(false, "Snapshot file is missing.", full);

        try
        {
            var loaded = await SnapshotReader.LoadStrictAsync<object?>(full, true, cancellationToken).ConfigureAwait(false);
            return new StorageVerificationResult(
                true,
                $"Snapshot is valid. Entries: {loaded.Entries.Count}; idempotency records: {loaded.IdempotencyRecords.Count}.",
                null);
        }
        catch (Exception ex) when (ex is InvalidDataException or JsonException or IOException)
        {
            return new StorageVerificationResult(false, "Snapshot is invalid.", ex.Message);
        }
    }
}
