using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Publishes immutable snapshots through one two-slot pointer file.</summary>
/// <remarks>The owning group log serializes publication and retains WAL from the preceding snapshot.</remarks>
internal sealed class GroupSnapshotCheckpointStore
{
    private readonly string _directory;
    private readonly string _groupId;
    private readonly int _maximum;
    private readonly IFollowerLogFaultHooks? _faults;
    private readonly ConcurrentDictionary<string, byte> _preparing = new(StringComparer.Ordinal);

    internal GroupSnapshotCheckpointStore(string directory, string groupId, int maximum, IFollowerLogFaultHooks? faults)
    {
        _directory = directory;
        _groupId = groupId;
        _maximum = maximum;
        _faults = faults;
    }

    internal string PointerPath => Path.Join(_directory, "group.snapshot.meta");

    /// <summary>Writes a verified generation without modifying either pointer slot.</summary>
    /// <param name="snapshot">The snapshot to prepare.</param>
    /// <param name="previousIndex">The expected fallback boundary.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The verified generation, or null if the fallback changed.</returns>
    internal async Task<PreparedSnapshot?> PrepareAsync(GroupSnapshot snapshot, ulong previousIndex, CancellationToken cancellationToken)
    {
        var pointers = GroupSnapshotPointers.Read(PointerPath);
        var (current, index) = await SelectCurrentAsync(pointers, cancellationToken).ConfigureAwait(false);
        if (previousIndex != index)
            return null;

        var needsSentinel = Array.TrueForAll(pointers, static pointer => pointer.Sequence == 0UL);
        if (needsSentinel)
            current = new SnapshotPointer(1UL, Guid.Empty, 0);

        var generation = Guid.NewGuid();
        var pointer = NextPointer(ref snapshot, pointers, in current, generation);
        var path = GenerationPath(generation);
        _ = _preparing.TryAdd(path, 0);
        try
        {
            await WriteGenerationAsync(generation, snapshot, cancellationToken).ConfigureAwait(false);
            var verified = GroupSnapshotStore.CanonicalizeVerifiedSnapshot(in snapshot)
                ?? await ReadGenerationAsync(pointer, cancellationToken).ConfigureAwait(false)
                ?? ThrowHelper.Throw<GroupSnapshot>(new InvalidDataException("The new snapshot failed durable readback verification."));
            verified = verified with { WalPosition = pointer.WalPosition };
            return new PreparedSnapshot(this, path, in verified, pointers, in pointer, needsSentinel);
        }
        catch
        {
            DiscardPreparation(path, false);
            throw;
        }
    }

    /// <summary>Publishes a prepared generation while the owning log serializes changes to the pointers.</summary>
    /// <param name="prepared">The verified generation.</param>
    /// <returns>Whether the prepared generation was published.</returns>
    internal bool TryPublishPrepared(PreparedSnapshot prepared)
    {
        var pointers = GroupSnapshotPointers.Read(PointerPath);
        if (!pointers.AsSpan().SequenceEqual(prepared.BasePointers))
            return false;

        if (prepared.NeedsSentinel)
            WritePointer(new SnapshotPointer(1UL, Guid.Empty, 0));

        _faults?.OnSnapshotPublishing();

        // A failed pointer flush is ambiguous. Keep the file so startup can resolve the publication.
        prepared.PointerAttempted = true;
        WritePointer(prepared.Pointer);
        DeleteUnreferenced(GroupSnapshotPointers.Read(PointerPath));
        return true;
    }

    internal async Task<GroupSnapshot?> ReadAsync(CancellationToken cancellationToken)
    {
        var pointers = GroupSnapshotPointers.Read(PointerPath);
        foreach (var pointer in pointers)
        {
            if (pointer.Sequence != 0UL && pointer.Generation == Guid.Empty)
                return null;

            var snapshot = await ReadGenerationAsync(pointer, cancellationToken).ConfigureAwait(false);
            if (snapshot != null)
                return snapshot.Value with { WalPosition = pointer.WalPosition };
        }

        return pointers.Length == 0 || Array.TrueForAll(pointers, static pointer => pointer.Sequence == 0UL)
            ? null
            : throw new InvalidDataException("No referenced snapshot generation is valid.");
    }

    internal async Task<GroupSnapshot> PublishAsync(GroupSnapshot snapshot, bool mirrored, ulong? previousIndex, CancellationToken cancellationToken)
    {
        var pointers = GroupSnapshotPointers.Read(PointerPath);
        if (!mirrored && previousIndex.GetValueOrDefault() == 0UL && Array.TrueForAll(pointers, static pointer => pointer.Sequence == 0UL))
        {
            _ = Directory.CreateDirectory(_directory);
            WritePointer(new SnapshotPointer(1UL, Guid.Empty, 0));
            pointers = GroupSnapshotPointers.Read(PointerPath);
        }

        var (current, index) = await SelectCurrentAsync(pointers, cancellationToken).ConfigureAwait(false);
        if (previousIndex is { } expected && expected != index)
            throw new InvalidDataException("The current snapshot is unavailable as a fallback for the compacted WAL.");

        if (mirrored)
            snapshot = snapshot with { WalPosition = default };

        _ = Directory.CreateDirectory(_directory);
        DeleteUnreferenced(pointers);
        var generation = Guid.NewGuid();
        var pointer = NextPointer(ref snapshot, pointers, in current, generation);
        await WriteGenerationAsync(generation, snapshot, cancellationToken).ConfigureAwait(false);
        if (!mirrored)
            _faults?.OnSnapshotPublishing();

        WritePointer(in pointer);
        if (mirrored)
        {
            // A state installation cannot recover from its previous local snapshot.
            // Finish the mirror even when the caller cancels after the first pointer flush.
            var mirror = Guid.NewGuid();
            await WriteGenerationAsync(mirror, snapshot, CancellationToken.None).ConfigureAwait(false);
            WritePointer(new SnapshotPointer(checked(pointer.Sequence + 1UL), mirror, 1 - pointer.Slot));
        }

        DeleteUnreferenced(GroupSnapshotPointers.Read(PointerPath));
        return snapshot;
    }

    private static SnapshotPointer NextPointer(ref GroupSnapshot snapshot, SnapshotPointer[] pointers, in SnapshotPointer current, Guid generation)
    {
        var sequence = current.Sequence;
        var position = snapshot.WalPosition;
        var reservedGeneration = position.Generation;
        foreach (var pointer in pointers)
        {
            sequence = Math.Max(sequence, pointer.Sequence);
            reservedGeneration = Math.Max(reservedGeneration, pointer.WalPosition.RewrittenGeneration);
        }

        // A sealed WAL must identify the particular rewrite, even after an interruption before sealing.
        if (position.RewrittenOffset > 0)
        {
            position.RewrittenGeneration = checked(reservedGeneration + 1UL);
            snapshot = snapshot with { WalPosition = position };
        }

        var target = current.Sequence == 0UL ? 0 : 1 - current.Slot;
        return new SnapshotPointer(checked(sequence + 1UL), generation, target) { WalPosition = position };
    }

    private async Task<(SnapshotPointer Pointer, ulong Index)> SelectCurrentAsync(SnapshotPointer[] pointers, CancellationToken cancellationToken)
    {
        foreach (var pointer in pointers)
        {
            if (pointer.Sequence != 0UL && pointer.Generation == Guid.Empty)
                return (pointer, 0UL);

            var snapshot = await ReadGenerationAsync(pointer, cancellationToken, false).ConfigureAwait(false);
            if (snapshot != null)
                return (pointer, snapshot.Value.LastIncludedIndex);
        }

        return default;
    }

    private async Task<GroupSnapshot?> ReadGenerationAsync(SnapshotPointer pointer, CancellationToken cancellationToken, bool materialize = true)
    {
        if (pointer.Sequence == 0UL || pointer.Generation == Guid.Empty)
            return null;

        var path = GenerationPath(pointer.Generation);
        if (!File.Exists(path))
            return null;

        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.Asynchronous);
        var length = RandomAccess.GetLength(handle);
        if (length > _maximum || length <= 0)
            return null;

        using var rented = MemoryPool<byte>.Shared.Rent(int.CreateChecked(length));
        var bytes = rented.Memory[..int.CreateChecked(length)];
        var read = 0;
        while (read < bytes.Length)
        {
            var count = await RandomAccess.ReadAsync(handle, bytes[read..], read, cancellationToken).ConfigureAwait(false);
            if (count == 0)
                return null;

            read += count;
        }

        if (!GroupSnapshotStore.TryDecodePublished(bytes.Span, _maximum, out var snapshot, out _, materialize))
            return null;
        return string.Equals(snapshot.GroupId, _groupId, StringComparison.Ordinal)
            ? snapshot
            : throw new InvalidDataException("The published snapshot belongs to a different group.");
    }

    private async Task WriteGenerationAsync(Guid generation, GroupSnapshot snapshot, CancellationToken cancellationToken)
    {
        var length = GroupSnapshotStore.ComputePublishedLength(in snapshot, _maximum);
        using var rented = MemoryPool<byte>.Shared.Rent(length);
        var bytes = rented.Memory[..length];
        GroupSnapshotStore.EncodePublished(bytes.Span, in snapshot);
        _faults?.OnSnapshotPublishing();
        _ = Directory.CreateDirectory(_directory);
        var path = GenerationPath(generation);
        using (var handle = FileEx.OpenPrivate(path, FileMode.CreateNew, FileAccess.Write))
        {
            FileEx.FlushDirectoryEntry(path);
            await RandomAccess.WriteAsync(handle, bytes, 0, cancellationToken).ConfigureAwait(false);
            RandomAccess.FlushToDisk(handle);
        }

        var verified = await ReadGenerationAsync(new SnapshotPointer(1UL, generation, 0), cancellationToken, false).ConfigureAwait(false);
        _ = verified ?? ThrowHelper.Throw<GroupSnapshot>(new InvalidDataException("The new snapshot failed durable readback verification."));
    }

    private void WritePointer(in SnapshotPointer pointer)
    {
        using var handle = FileEx.OpenPrivate(PointerPath, FileMode.OpenOrCreate, FileAccess.Write);
        FileEx.FlushDirectoryEntry(PointerPath);
        Span<byte> slot = stackalloc byte[GroupSnapshotPointers.SlotSize];
        GroupSnapshotPointers.Encode(slot, pointer.Sequence, pointer.Generation, pointer.WalPosition);
        RandomAccess.Write(handle, slot, pointer.Slot * GroupSnapshotPointers.SlotSize);
        RandomAccess.FlushToDisk(handle);
    }

    private void DeleteUnreferenced(SnapshotPointer[] pointers)
    {
        foreach (var path in Directory.EnumerateFiles(_directory, "group.checkpoint-*.snapshot"))
        {
            var referenced = false;
            foreach (var pointer in pointers)
                referenced |= pointer.Sequence != 0UL && string.Equals(path, GenerationPath(pointer.Generation), StringComparison.Ordinal);
            if (!referenced && !_preparing.ContainsKey(path))
                _ = FileEx.TryDeleteFile(path);
        }
    }

    private string GenerationPath(Guid generation) => Path.Join(_directory, $"group.checkpoint-{generation:N}.snapshot");

    private void DiscardPreparation(string path, bool pointerAttempted)
    {
        _ = _preparing.TryRemove(path, out _);
        if (!pointerAttempted)
            _ = FileEx.TryDeleteFile(path);
    }

    /// <summary>A verified file and the pointer state against which it was prepared.</summary>
    internal sealed class PreparedSnapshot : IDisposable
    {
        private readonly GroupSnapshotCheckpointStore _store;
        private readonly string _path;

        internal PreparedSnapshot(GroupSnapshotCheckpointStore store, string path, in GroupSnapshot snapshot, SnapshotPointer[] basePointers, in SnapshotPointer pointer, bool needsSentinel)
        {
            _store = store;
            _path = path;
            Snapshot = snapshot;
            BasePointers = basePointers;
            Pointer = pointer;
            NeedsSentinel = needsSentinel;
        }

        internal GroupSnapshot Snapshot { get; }
        internal SnapshotPointer[] BasePointers { get; }
        internal SnapshotPointer Pointer { get; }
        internal bool NeedsSentinel { get; }
        internal bool PointerAttempted { get; set; }

        /// <inheritdoc />
        public void Dispose() => _store.DiscardPreparation(_path, PointerAttempted);
    }
}
