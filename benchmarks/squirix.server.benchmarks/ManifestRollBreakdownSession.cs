using System;
using System.Buffers;
using System.Globalization;
using System.Text;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Hosts a warmed manifest store for roll-path breakdown benchmarks.</summary>
internal sealed class ManifestRollBreakdownSession : IDisposable
{
    private const int EncodeBufferSize = 512;

    private readonly TempDirectory _dataDir;
    private readonly byte[] _encodeBuffer;

    private ManifestRollBreakdownSession(
        TempDirectory dataDir,
        ManifestStore store,
        int format,
        ManifestState.SnapshotRef? snapshot,
        byte[] snapshotPathUtf8,
        byte[] encodeBuffer,
        string manifestFileNamePrefix,
        IManifestPointerWriter pointerWriter)
    {
        _dataDir = dataDir;
        _encodeBuffer = encodeBuffer;
        Store = store;
        Format = format;
        Snapshot = snapshot;
        SnapshotPathUtf8 = snapshotPathUtf8;
        ManifestFileNamePrefix = manifestFileNamePrefix;
        PointerWriter = pointerWriter;
    }

    public ManifestStore Store { get; }

    private int Format { get; }

    private string ManifestFileNamePrefix { get; }

    private IManifestPointerWriter PointerWriter { get; }

    private ManifestState.SnapshotRef? Snapshot { get; }

    private byte[] SnapshotPathUtf8 { get; }

    /// <summary>Creates a warmed manifest session with primed in-memory cache.</summary>
    /// <returns>A session ready for breakdown benchmarks.</returns>
    public static ManifestRollBreakdownSession Create()
    {
        var dataDir = new TempDirectory("manifest-breakdown");
        var retention = ManifestBenchmarkSupport.ResolveRetentionCount();
        var options = new PersistenceOptions
        {
            DataDir = dataDir.Path,
            ManifestRetentionCount = retention,
            SnapshotRetentionCount = retention,
        };
        var store = new ManifestStore(options);
        store.PublishRollBlocking(1, 1);

        var current = store.ReadCurrentOrDefaultBlocking();
        var snapshotPathUtf8 = current.LastSnapshot?.Path is { Length: > 0 } path ? Encoding.UTF8.GetBytes(path) : [];

        var encodeBuffer = ArrayPool<byte>.Shared.Rent(EncodeBufferSize);
        var manifestFileNamePrefix = PathEx.Combine(dataDir.Path, StorageFilePrefixes.Manifest);
        var currentPath = PathEx.Combine(dataDir.Path, $"{StorageFilePrefixes.Manifest}current");
        var pointerWriter = new ManifestPersistentPointerWriter(currentPath);

        return new ManifestRollBreakdownSession(
            dataDir,
            store,
            current.Format is 0 ? 1 : current.Format,
            current.LastSnapshot,
            snapshotPathUtf8,
            encodeBuffer,
            manifestFileNamePrefix,
            pointerWriter);
    }

    /// <summary>Builds a numbered manifest file path under the session data directory.</summary>
    /// <param name="index">One-based manifest file index.</param>
    /// <returns>Absolute path to a <c>.bmqx</c> file.</returns>
    public string BuildManifestFilePath(int index) => string.Create(
        ManifestFileNamePrefix.Length + 6 + StorageFileExtensions.Manifest.Length,
        (Prefix: ManifestFileNamePrefix, Index: index),
        static (span, state) =>
        {
            state.Prefix.CopyTo(span);
            var suffix = span[state.Prefix.Length..];
            if (!state.Index.TryFormat(suffix, out var charsWritten, "D6", CultureInfo.InvariantCulture))
                throw new InvalidOperationException("Manifest index did not fit fixed-width field.");

            StorageFileExtensions.Manifest.CopyTo(suffix[charsWritten..]);
        });

    /// <summary>Encodes a segment-roll manifest into the session encode buffer.</summary>
    /// <param name="currentJournal">Updated current journal segment index.</param>
    /// <param name="nextSequence">Updated next journal sequence.</param>
    /// <returns>Encoded byte length.</returns>
    public int EncodeRoll(int currentJournal, ulong nextSequence) =>
        ManifestCodec.WriteRollEncoded(Format, currentJournal, nextSequence, Snapshot, SnapshotPathUtf8, _encodeBuffer);

    /// <summary>Writes a pre-encoded manifest file and flushes it to disk.</summary>
    /// <param name="targetPath">Path to a new <c>.bmqx</c> file.</param>
    /// <param name="encodedLength">Number of valid bytes in the session encode buffer.</param>
    public void WriteDataFile(string targetPath, int encodedLength) => ManifestDurability.WriteManifestDataFileBlocking(targetPath, _encodeBuffer.AsSpan(0, encodedLength));

    /// <summary>Writes the SQMC current pointer and flushes it to disk.</summary>
    /// <param name="manifestIndex">Manifest index for the pointer payload.</param>
    public void WritePointer(int manifestIndex)
    {
        Span<byte> pointerBuffer = stackalloc byte[ManifestPointer.Size];
        ManifestPointer.Write(pointerBuffer, manifestIndex);
        ManifestDurability.WriteCurrentPointerBlocking(PointerWriter, pointerBuffer);
    }

    public void Dispose()
    {
        ArrayPool<byte>.Shared.Return(_encodeBuffer);
        PointerWriter.Dispose();
        Store.Dispose();
        _dataDir.Dispose();
    }
}
