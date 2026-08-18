using System;
using System.Globalization;
using System.IO;
using System.Threading;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Owns and resolves the next numbered manifest file index from cache, disk scan, or CURRENT pointer.</summary>
internal sealed class IndexAllocator
{
    private readonly string _currentPath;
    private readonly string _dataDir;
    private readonly string _manifestFileGlob;
    private readonly string _manifestFileNamePrefix;
    private readonly Lock _nextIndexInitLock = new();
    private readonly NextIndex _nextManifestIndex = new();
    private readonly Func<int?> _readCurrentIndexForInit;
    private volatile bool _nextIndexInitialized;

    internal IndexAllocator(string dataDir, string currentPath, string manifestFileNamePrefix, string manifestFileGlob, Func<int?> readCurrentIndexForInit)
    {
        _dataDir = dataDir;
        _currentPath = currentPath;
        _manifestFileNamePrefix = manifestFileNamePrefix;
        _manifestFileGlob = manifestFileGlob;
        _readCurrentIndexForInit = readCurrentIndexForInit;
    }

    internal static int ParseManifestIndex(string name) => ParseManifestIndex(name.AsSpan());

    internal int AllocateNextManifestIndex()
    {
        EnsureNextManifestIndexInitialized(CancellationToken.None);
        return IncrementNextManifestIndex();
    }

    internal string BuildManifestFilePath(int index) => string.Create(
        _manifestFileNamePrefix.Length + 6 + FileExtensions.Manifest.Length,
        (Prefix: _manifestFileNamePrefix, Index: index),
        static (span, state) =>
        {
            state.Prefix.CopyTo(span);
            var suffix = span[state.Prefix.Length..];
            if (!state.Index.TryFormat(suffix, out var charsWritten, "D6", CultureInfo.InvariantCulture))
                throw new InvalidOperationException("Manifest index did not fit fixed-width field.");

            FileExtensions.Manifest.CopyTo(suffix[charsWritten..]);
        });

    internal void EnsureNextManifestIndexInitialized(CancellationToken cancellationToken)
    {
        if (_nextIndexInitialized)
            return;

        lock (_nextIndexInitLock)
        {
            if (_nextIndexInitialized)
                return;

            var fromCache = _readCurrentIndexForInit();
            if (fromCache != null)
            {
                _nextManifestIndex.Set(fromCache.Value);
                _nextIndexInitialized = true;
                return;
            }
        }

        var nextFromDisk = ResolveNextIndexFromDisk(cancellationToken);

        lock (_nextIndexInitLock)
        {
            if (_nextIndexInitialized)
                return;

            _nextManifestIndex.Set(nextFromDisk - 1);
            _nextIndexInitialized = true;
        }
    }

    internal int IncrementNextManifestIndex()
    {
        lock (_nextIndexInitLock)
            return _nextManifestIndex.Increment();
    }

    internal void SeedNextManifestIndex(int publishedIndex)
    {
        lock (_nextIndexInitLock)
        {
            _nextManifestIndex.Set(publishedIndex);
            _nextIndexInitialized = true;
        }
    }

    private static int ParseManifestIndex(ReadOnlySpan<char> name)
    {
        if (name.IsEmpty)
            return 0;

        var prefix = FilePrefixes.Manifest.AsSpan();
        var extension = FileExtensions.Manifest.AsSpan();
        if (!name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return 0;

        if (!name.EndsWith(extension, StringComparison.OrdinalIgnoreCase))
            return 0;

        var numberPart = name.Slice(prefix.Length, name.Length - prefix.Length - extension.Length);
        return int.TryParse(numberPart, CultureInfo.InvariantCulture, out var n) ? n : 0;
    }

    private static int ResolveNextIndexFromPointer(int fromCurrent, int maxOnDisk)
    {
        var baseline = fromCurrent > maxOnDisk ? fromCurrent : maxOnDisk;
        return baseline + 1;
    }

    private int ResolveNextIndexFromDisk(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var maxOnDisk = ScanMaxManifestIndexOnDisk();
        if (!File.Exists(_currentPath))
            return maxOnDisk + 1;

        return ResolveNextIndexFromPointer(PointerFile.ReadIndex(_currentPath), maxOnDisk);
    }

    private int ScanMaxManifestIndexOnDisk()
    {
        if (!Directory.Exists(_dataDir))
            return 0;

        var max = 0;
        foreach (var path in Directory.GetFiles(_dataDir, _manifestFileGlob))
        {
            var index = ParseManifestIndex(Path.GetFileName(path));
            if (index > max)
                max = index;
        }

        return max;
    }

    /// <summary>Mutable next-manifest index value; keeps assignments off <see cref="IndexAllocator" /> for ND1906.</summary>
    private sealed class NextIndex
    {
        private int _value;

        internal int Increment() => ++_value;

        internal void Set(int value) => _value = value;
    }
}
