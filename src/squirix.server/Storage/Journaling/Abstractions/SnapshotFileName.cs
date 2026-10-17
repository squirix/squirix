using System;
using System.Globalization;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Parses snapshot file names.</summary>
internal static class SnapshotFileName
{
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
