using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Encodes and durably writes manifest files and the fixed-size CURRENT pointer.</summary>
[SuppressMessage(
    "AsyncUsage",
    "MA0045:Do not use blocking calls in a sync method",
    Justification = "Blocking manifest file I/O runs on the dedicated journal I/O thread without a synchronization context.")]
internal sealed class Publisher : IDisposable
{
    private const int DefaultEncodeBufferCapacity = 256;

    private static readonly Action<object?> WritePublishedManifestBlockingCallback = static state =>
    {
        if (state is Publisher publisher)
            publisher.WritePublishedManifestBlocking();
    };

    private readonly IndexAllocator _allocator;
    private readonly byte[] _currentPointerBuffer = new byte[Pointer.Size];
    private readonly PersistentPointerWriter _currentPointerWriter;

    private readonly string _dataDir;
    private readonly Func<(State Previous, ReadOnlyMemory<byte> SnapshotPathUtf8)> _readRollBaselineLocked;
    private readonly Action<State, int> _setCache;
    private bool _dataDirEnsured;
    private byte[] _encodeBuffer = new byte[DefaultEncodeBufferCapacity];
    private PublishWork? _publishWork;

    internal Publisher(
        string dataDir,
        string currentPath,
        IndexAllocator allocator,
        Action<State, int> setCache,
        Func<(State Previous, ReadOnlyMemory<byte> SnapshotPathUtf8)> readRollBaselineLocked)
    {
        _dataDir = dataDir;
        _allocator = allocator;
        _setCache = setCache;
        _readRollBaselineLocked = readRollBaselineLocked;
        _currentPointerWriter = new PersistentPointerWriter(currentPath);
    }

    public void Dispose() => _currentPointerWriter.Dispose();

    internal async Task EnsureDataDirectoryExistsAsync(CancellationToken cancellationToken)
    {
        if (_dataDirEnsured)
            return;

        _ = await DirectoryEx.CreateDirectoryAsync(_dataDir, cancellationToken: cancellationToken).ConfigureAwait(false);
        _dataDirEnsured = true;
    }

    internal async Task PublishCoreAsync(State manifest, int nextIndex, CancellationToken cancellationToken)
    {
        await EnsureDataDirectoryExistsAsync(cancellationToken).ConfigureAwait(false);

        var targetPath = _allocator.BuildManifestFilePath(nextIndex);
        var encodedLength = FileCodec.ComputeEncodedLength(manifest);
        EnsureEncodeBufferCapacity(encodedLength);

        FileCodec.WriteEncoded(manifest, _encodeBuffer.AsSpan(0, encodedLength));

        _publishWork = new PublishWork(targetPath, encodedLength, nextIndex);

        await Task.Factory.StartNew(WritePublishedManifestBlockingCallback, this, cancellationToken, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default)
                  .ConfigureAwait(false);

        _setCache(manifest, nextIndex);
    }

    internal State PublishRollCoreBlocking(int currentJournal, ulong nextSequence, int nextIndex)
    {
        EnsureDataDirectoryExists();

        var (previous, snapshotPathUtf8) = _readRollBaselineLocked();
        var format = previous.Format is 0 ? 1 : previous.Format;
        var snapshot = previous.LastSnapshot;

        var encodedLength = FileCodec.ComputeRollEncodedLength(snapshot, snapshotPathUtf8.Length);
        EnsureEncodeBufferCapacity(encodedLength);

        _ = FileCodec.WriteRollEncoded(format, currentJournal, nextSequence, snapshot, snapshotPathUtf8.Span, _encodeBuffer.AsSpan(0, encodedLength));

        var targetPath = _allocator.BuildManifestFilePath(nextIndex);
        Pointer.Write(_currentPointerBuffer, nextIndex);
        FileDurability.WriteManifestRollBlocking(targetPath, _encodeBuffer.AsSpan(0, encodedLength), _currentPointerWriter, _currentPointerBuffer);

        var manifest = new State
        {
            Format = format,
            CurrentJournal = currentJournal,
            NextSequence = nextSequence,
            LastSnapshot = snapshot,
        };
        _setCache(manifest, nextIndex);
        return manifest;
    }

    private void EnsureDataDirectoryExists()
    {
        if (_dataDirEnsured)
            return;

        _ = Directory.CreateDirectory(_dataDir);
        _dataDirEnsured = true;
    }

    private void EnsureEncodeBufferCapacity(int encodedLength)
    {
        if (_encodeBuffer.Length >= encodedLength)
            return;

        _encodeBuffer = new byte[Math.Max(encodedLength, _encodeBuffer.Length * 2)];
    }

    private void UpdateCurrentPointerBlocking(int manifestIndex)
    {
        Pointer.Write(_currentPointerBuffer, manifestIndex);
        FileDurability.WriteCurrentPointerBlocking(_currentPointerWriter, _currentPointerBuffer);
    }

    private void WritePublishedManifestBlocking()
    {
        if (_publishWork is not { } publishWork)
            throw new InvalidOperationException("Publish work was not initialized.");

        FileDurability.WriteManifestDataFileBlocking(publishWork.TargetPath, _encodeBuffer.AsSpan(0, publishWork.EncodedLength));
        UpdateCurrentPointerBlocking(publishWork.ManifestIndex);
    }

    private sealed record PublishWork(string TargetPath, int EncodedLength, int ManifestIndex);
}
