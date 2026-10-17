using System;
using System.IO;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Durable writer for the two-slot <c language="csharp">man-current</c> pointer; the layout lives in <see cref="PointerSlots" />.</summary>
/// <remarks>
///     <para>
///     The file is created and written in place through one persistent handle, never through a temporary file. A publish writes the new record into
///     the slot that does not hold the current one and flushes it, so the previous record stays intact whenever a write is torn or lost; the pointer
///     counts as moved once that flush returned. <see cref="Mirror" /> then writes the same index into the other slot, so that damage at rest to one
///     slot loses nothing. The caller acknowledges a publish only after the mirror.
///     </para>
///     <para>
///     <see cref="Publish" /> and <see cref="Mirror" /> are called serially by the manifest publisher worker; <see cref="Dispose" /> may run on another
///     thread and refuses every later write.
///     </para>
/// </remarks>
[Mutable]
internal sealed class PersistentPointerWriter : IDisposable
{
    private readonly Lock _gate = new();
    private readonly string _path;
    private readonly byte[] _record = new byte[PointerSlots.RecordSize];
    private int _currentIndex;
    private int _currentSlot;
    private int _disposed;
    private SafeFileHandle? _handle;
    private ulong _sequence;

    internal PersistentPointerWriter(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        _path = path;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _ = Interlocked.Exchange(ref _disposed, 1);
        lock (_gate)
        {
            _handle?.Dispose();
            _handle = null;
        }
    }

    /// <summary>Durably moves the pointer to a manifest, leaving the other slot on the previous record.</summary>
    /// <param name="manifestIndex">The index of the manifest file, which must already be durable.</param>
    /// <exception cref="ObjectDisposedException">The writer was disposed.</exception>
    /// <exception cref="InvalidDataException">The existing file was written by an earlier build or holds no valid slot; it is left unchanged.</exception>
    internal void Publish(int manifestIndex)
    {
        var handle = EnsureOpen();
        WriteInactiveSlot(handle, manifestIndex);
    }

    /// <summary>Durably writes the current index into the other slot too.</summary>
    /// <exception cref="ObjectDisposedException">The writer was disposed.</exception>
    internal void Mirror()
    {
        var handle = EnsureOpen();
        WriteInactiveSlot(handle, _currentIndex);
    }

    private SafeFileHandle EnsureOpen()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (_handle is { } open)
                return open;

            var options = OperatingSystem.IsWindows() ? FileOptions.WriteThrough : FileOptions.None;
            var handle = File.OpenHandle(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, options);
            try
            {
                // The directory entry of a file created now, or by an earlier crashed first publish, is flushed before any record relies on it.
                FileEx.FlushDirectoryEntry(_path);
                var selection = PointerFile.Read(handle, _path);
                if (RandomAccess.GetLength(handle) < PointerSlots.FileSize)
                    RandomAccess.SetLength(handle, PointerSlots.FileSize);

                _currentSlot = selection.Slot;
                _sequence = selection.Sequence;
                _currentIndex = selection.ManifestIndex;
            }
            catch
            {
                handle.Dispose();
                throw;
            }

            _handle = handle;
            return handle;
        }
    }

    private void WriteInactiveSlot(SafeFileHandle handle, int manifestIndex)
    {
        var target = 1 - _currentSlot;
        var next = _sequence + 1UL;
        PointerSlots.Encode(_record, next, manifestIndex);
        long offset = target == 0 ? 0 : PointerSlots.SlotSize;
        RandomAccess.Write(handle, _record, offset);
        RandomAccess.FlushToDisk(handle);
        _currentSlot = target;
        _sequence = next;
        _currentIndex = manifestIndex;
    }
}
