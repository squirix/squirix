using System;
using System.Globalization;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Parses snapshot file names.</summary>
internal static class SnapshotFileName
{
    /// <summary>Extracts the index from a snapshot file name such as <c language="csharp">snp-000005.bsqx</c>.</summary>
    /// <param name="name">File name without directory.</param>
    /// <returns>The index, or 0 when the name is not a snapshot file name.</returns>
    internal static int ParseIndex(string name) => ParseIndex(name.AsSpan(), FileExtensions.Snapshot.AsSpan());

    private static int ParseIndex(ReadOnlySpan<char> name, ReadOnlySpan<char> extension)
    {
        if (name.IsEmpty)
            return 0;
        if (!name.StartsWith(FilePrefixes.Snapshot.AsSpan(), StringComparison.OrdinalIgnoreCase))
            return 0;
        if (!name.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = name.Slice(FilePrefixes.Snapshot.Length, name.Length - FilePrefixes.Snapshot.Length - extension.Length);
        return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
    }
}
