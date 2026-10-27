using System;
using System.Globalization;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Builds and parses snapshot file names.</summary>
internal static class SnapshotFileName
{
    /// <summary>Builds the path of the snapshot file with the given index.</summary>
    /// <param name="dataDir">The data directory.</param>
    /// <param name="index">The snapshot index.</param>
    /// <returns>The snapshot file path.</returns>
    internal static string BuildPath(string dataDir, int index) =>
        PathEx.Combine(dataDir, $"{FilePrefixes.Snapshot}{InvariantDigitStrings.FormatD6(index)}{FileExtensions.Snapshot}");

    /// <summary>Extracts the index from a snapshot file name such as <c language="csharp">snp-000005.bsqx</c>.</summary>
    /// <param name="name">File name without directory.</param>
    /// <returns>The index, or 0 when the name is not a snapshot file name.</returns>
    internal static int ParseIndex(string name)
    {
        var span = name.AsSpan();
        var extension = FileExtensions.Snapshot.AsSpan();
        if (span.IsEmpty)
            return 0;
        if (!span.StartsWith(FilePrefixes.Snapshot.AsSpan(), StringComparison.OrdinalIgnoreCase))
            return 0;
        if (!span.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = span.Slice(FilePrefixes.Snapshot.Length, span.Length - FilePrefixes.Snapshot.Length - extension.Length);
        return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
    }
}
