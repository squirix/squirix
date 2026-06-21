using System;
using System.Globalization;
using System.Text;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Manifest.Binary;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Hosts an warmed binary manifest store for roll-path breakdown benchmarks.</summary>
internal sealed class BinaryManifestRollBreakdownSession : IDisposable
{
    private readonly TempDirectory _dataDir;

    private BinaryManifestRollBreakdownSession(
        TempDirectory dataDir,
        ManifestStore store,
        int format,
        ManifestState.SnapshotRef? snapshot,
        byte[] snapshotPathUtf8,
        byte[] encodeBuffer,
        byte[] pointerBuffer,
        string manifestFileNamePrefix,
        IBinaryManifestPointerWriter pointerWriter)
    {
        _dataDir = dataDir;
        Store = store;
        Format = format;
        Snapshot = snapshot;
        SnapshotPathUtf8 = snapshotPathUtf8;
        EncodeBuffer = encodeBuffer;
        PointerBuffer = pointerBuffer;
        ManifestFileNamePrefix = manifestFileNamePrefix;
        PointerWriter = pointerWriter;
    }

    public ManifestStore Store { get; }

    private byte[] EncodeBuffer { get; }

    private int Format { get; }

    private string ManifestFileNamePrefix { get; }

    private byte[] PointerBuffer { get; }

    private IBinaryManifestPointerWriter PointerWriter { get; }

    private ManifestState.SnapshotRef? Snapshot { get; }

    private byte[] SnapshotPathUtf8 { get; }

    /// <summary>Creates a warmed binary manifest session with primed in-memory cache.</summary>
    /// <returns>A session ready for breakdown benchmarks.</returns>
    public static BinaryManifestRollBreakdownSession Create()
    {
        var dataDir = new TempDirectory("binary-manifest-breakdown");
        var retention = ManifestBenchmarkSupport.ResolveRetentionCount();
        var options = new PersistenceOptions
        {
            DataDir = dataDir.Path,
            ManifestBackend = ManifestBackend.Binary,
            ManifestRetentionCount = retention,
            SnapshotRetentionCount = retention,
        };
        var store = new ManifestStore(options);
        store.PublishRollBlocking(1, 1);

        var current = store.ReadCurrentOrDefaultBlocking();
        var snapshotPathUtf8 = current.LastSnapshot?.Path is { Length: > 0 } path ? Encoding.UTF8.GetBytes(path) : [];

        var encodeBuffer = new byte[512];
        var pointerBuffer = new byte[BinaryManifestPointer.Size];
        var manifestFileNamePrefix = PathEx.Combine(dataDir.Path, StorageFilePrefixes.Manifest);
        var currentPath = PathEx.Combine(dataDir.Path, $"{StorageFilePrefixes.Manifest}current");
        var pointerWriter = new BinaryManifestPersistentPointerWriter(currentPath);

        return new BinaryManifestRollBreakdownSession(
            dataDir,
            store,
            current.Format is 0 ? 1 : current.Format,
            current.LastSnapshot,
            snapshotPathUtf8,
            encodeBuffer,
            pointerBuffer,
            manifestFileNamePrefix,
            pointerWriter);
    }

    /// <summary>Builds a numbered manifest file path under the session data directory.</summary>
    /// <param name="index">One-based manifest file index.</param>
    /// <returns>Absolute path to a <c>.bmqx</c> file.</returns>
    public string BuildManifestFilePath(int index) => string.Create(
        ManifestFileNamePrefix.Length + 6 + StorageFileExtensions.BinaryManifest.Length,
        (Prefix: ManifestFileNamePrefix, Index: index),
        static (span, state) =>
        {
            state.Prefix.AsSpan().CopyTo(span);
            var suffix = span.Slice(state.Prefix.Length);
            if (!state.Index.TryFormat(suffix, out var charsWritten, "D6", CultureInfo.InvariantCulture))
                throw new InvalidOperationException("Manifest index did not fit fixed-width field.");

            StorageFileExtensions.BinaryManifest.AsSpan().CopyTo(suffix.Slice(charsWritten));
        });

    /// <summary>Encodes a segment-roll manifest into the session encode buffer.</summary>
    /// <param name="currentJournal">Updated current journal segment index.</param>
    /// <param name="nextSequence">Updated next journal sequence.</param>
    /// <returns>Encoded byte length.</returns>
    public int EncodeRoll(int currentJournal, ulong nextSequence) =>
        BinaryManifestCodec.WriteRollEncoded(Format, currentJournal, nextSequence, Snapshot, SnapshotPathUtf8, EncodeBuffer);

    /// <summary>Writes a pre-encoded manifest file and flushes it to disk.</summary>
    /// <param name="targetPath">Path to a new <c>.bmqx</c> file.</param>
    /// <param name="encodedLength">Number of valid bytes in <see cref="EncodeBuffer" />.</param>
    public void WriteDataFile(string targetPath, int encodedLength) => BinaryManifestDurability.WriteManifestDataFileBlocking(targetPath, EncodeBuffer.AsSpan(0, encodedLength));

    /// <summary>Writes the SQMC current pointer and flushes it to disk.</summary>
    /// <param name="manifestIndex">Manifest index for the pointer payload.</param>
    public void WritePointer(int manifestIndex)
    {
        BinaryManifestPointer.Write(PointerBuffer, manifestIndex);
        BinaryManifestDurability.WriteCurrentPointerBlocking(PointerWriter, PointerBuffer);
    }

    /// <summary>Durably publishes roll payload and pointer using the production roll durability path.</summary>
    /// <param name="targetPath">Path to a new <c>.bmqx</c> file.</param>
    /// <param name="encodedLength">Number of valid bytes in <see cref="EncodeBuffer" />.</param>
    /// <param name="manifestIndex">Manifest index for the pointer payload.</param>
    public void WriteRoll(string targetPath, int encodedLength, int manifestIndex)
    {
        BinaryManifestPointer.Write(PointerBuffer, manifestIndex);
        BinaryManifestDurability.WriteManifestRollBlocking(
            targetPath,
            EncodeBuffer.AsSpan(0, encodedLength),
            PointerWriter,
            PointerBuffer);
    }

    public void Dispose()
    {
        PointerWriter.Dispose();
        Store.Dispose();
        _dataDir.Dispose();
    }
}
