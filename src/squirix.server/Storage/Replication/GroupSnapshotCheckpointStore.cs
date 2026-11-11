using System;
using System.Buffers;
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

    internal GroupSnapshotCheckpointStore(string directory, string groupId, int maximum, IFollowerLogFaultHooks? faults)
    {
        _directory = directory;
        _groupId = groupId;
        _maximum = maximum;
        _faults = faults;
    }

    internal string PointerPath => Path.Join(_directory, "group.snapshot.meta");

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

        var sequence = 0UL;
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

        _ = Directory.CreateDirectory(_directory);
        DeleteUnreferenced(pointers);
        var generation = Guid.NewGuid();
        await WriteGenerationAsync(generation, snapshot, cancellationToken).ConfigureAwait(false);
        var target = current.Sequence == 0UL ? 0 : 1 - current.Slot;
        if (!mirrored)
            _faults?.OnSnapshotPublishing();

        WritePointer(new SnapshotPointer(checked(sequence + 1UL), generation, target) { WalPosition = snapshot.WalPosition });
        if (mirrored)
        {
            // A state installation cannot recover from its previous local snapshot.
            // Finish the mirror even when the caller cancels after the first pointer flush.
            var mirror = Guid.NewGuid();
            await WriteGenerationAsync(mirror, snapshot, CancellationToken.None).ConfigureAwait(false);
            WritePointer(new SnapshotPointer(checked(sequence + 2UL), mirror, 1 - target));
        }

        DeleteUnreferenced(GroupSnapshotPointers.Read(PointerPath));
        return snapshot;
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
            if (!referenced)
                _ = FileEx.TryDeleteFile(path);
        }
    }

    private string GenerationPath(Guid generation) => Path.Join(_directory, $"group.checkpoint-{generation:N}.snapshot");
}
