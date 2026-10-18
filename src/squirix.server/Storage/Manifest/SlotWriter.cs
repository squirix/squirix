using System;
using System.IO;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Durable writer for the two-slot <c language="csharp">man-current</c> manifest file; the layout lives in <see cref="SlotFormat" />.</summary>
/// <remarks>
///     <para>
///     The file is created and written in place through one persistent handle, never through a temporary file. A publish writes the new manifest into
///     the slot that does not hold the current one and flushes it, so the previous manifest stays intact whenever a write is torn or lost; the manifest
///     counts as moved once that flush returned. <see cref="Mirror" /> then writes the same manifest into the other slot, so that damage at rest to one
///     slot loses nothing. The caller acknowledges a publish only after the mirror. A publish or mirror that throws after
///     the write began may have landed, so its outcome is unknown and it must not be retried as if nothing was written.
///     </para>
///     <para>
///     <see cref="Publish" /> and <see cref="Mirror" /> are called serially by the manifest publisher worker; <see cref="Dispose" /> may run on another
///     thread and refuses every later write.
///     </para>
/// </remarks>
[Mutable]
internal sealed class SlotWriter : IDisposable
{
    private readonly string _path;
    private readonly byte[] _record = new byte[SlotFormat.RecordSize];
    private State? _currentManifest;
    private int _currentSlot;
    private int _disposed;
    private SafeFileHandle? _handle;
    private bool _outcomeUnknown;
    private ulong _sequence;

    internal SlotWriter(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        _path = path;
    }

    /// <summary>Gets a value indicating whether a write began that did not complete, so a slot may hold its manifest although the call threw.</summary>
    internal bool OutcomeUnknown => Volatile.Read(ref _outcomeUnknown);

    /// <inheritdoc />
    public void Dispose()
    {
        _ = Interlocked.Exchange(ref _disposed, 1);
        _handle?.Dispose();
    }

    /// <summary>Durably writes the current manifest into the other slot too.</summary>
    /// <exception cref="ObjectDisposedException">The writer was disposed.</exception>
    /// <exception cref="InvalidOperationException">The manifest has never been published.</exception>
    internal void Mirror()
    {
        EnsureOpen();
        _ = WriteInactiveSlot(ThrowHelper.Required(_currentManifest, "The manifest has not been published, so there is nothing to mirror."));
    }

    /// <summary>Durably moves the manifest to a new state, leaving the other slot on the previous one.</summary>
    /// <param name="manifest">The manifest to store.</param>
    /// <returns>The sequence of the slot record that now holds the manifest.</returns>
    /// <exception cref="ObjectDisposedException">The writer was disposed.</exception>
    /// <exception cref="ArgumentException">The manifest is one a reader would reject; nothing was written.</exception>
    /// <exception cref="InvalidDataException">The existing file was written by an earlier build or holds no valid slot; it is left unchanged.</exception>
    internal ulong Publish(State manifest)
    {
        EnsureOpen();
        return WriteInactiveSlot(manifest);
    }

    /// <summary>Gets the manifest the slots hold, as selected when the file was opened or as of the last write this writer completed.</summary>
    /// <returns>The current manifest, or the default manifest when none was ever published.</returns>
    /// <exception cref="ObjectDisposedException">The writer was disposed.</exception>
    /// <exception cref="InvalidDataException">The existing file was written by an earlier build or holds no valid slot; it is left unchanged.</exception>
    internal State ReadCurrentOrDefault()
    {
        EnsureOpen();
        return _currentManifest ?? new State();
    }

    private void EnsureOpen()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (Volatile.Read(ref _handle) != null)
            return;

        var handle = File.OpenHandle(_path, FileMode.OpenOrCreate, FileAccess.ReadWrite);
        try
        {
            // The directory entry of a file created now, or by an earlier crashed first publish, is flushed before any record relies on it.
            FileEx.FlushDirectoryEntry(_path);
            var selection = SlotFile.Read(handle, _path);
            if (RandomAccess.GetLength(handle) < SlotFormat.FileSize)
                RandomAccess.SetLength(handle, SlotFormat.FileSize);

            _currentSlot = selection.Slot;
            _sequence = selection.Sequence;
            _currentManifest = selection.Sequence == 0UL ? null : selection.State;
        }
        catch
        {
            handle.Dispose();
            throw;
        }

        _handle = handle;
        Interlocked.MemoryBarrier();

        // A dispose that ran while the file was being opened found no handle to close, so the new one is closed here.
        if (Volatile.Read(ref _disposed) == 0)
            return;

        handle.Dispose();
        throw new ObjectDisposedException(nameof(SlotWriter));
    }

    private ulong WriteInactiveSlot(State manifest)
    {
        // Closed by a racing dispose, the handle refuses the write with ObjectDisposedException.
        var handle = ThrowHelper.Required(_handle, "The manifest handle is not open.");
        var target = 1 - _currentSlot;

        // The sequence is spent before the write: bytes of a failed attempt may still land and be read, so a retry must never reuse it.
        var next = ++_sequence;
        SlotFormat.Encode(_record, manifest, next);
        long offset = target == 0 ? 0 : SlotFormat.SlotSize;

        // Set before the first byte is written and cleared only after the flush: a throw in between may have landed.
        Volatile.Write(ref _outcomeUnknown, true);
        RandomAccess.Write(handle, _record, offset);
        RandomAccess.FlushToDisk(handle);
        Volatile.Write(ref _outcomeUnknown, false);
        _currentSlot = target;
        _currentManifest = manifest;
        return next;
    }
}
