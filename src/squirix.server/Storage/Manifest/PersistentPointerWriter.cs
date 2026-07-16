using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Reusable <c>man-current</c> handle for the journal roll hot path.</summary>
internal sealed class PersistentPointerWriter : IManifestPointerWriter
{
    private readonly string _currentPath;
    private SafeFileHandle? _handle;

    public PersistentPointerWriter(string currentPath)
    {
        _currentPath = currentPath;
    }

    public void Dispose() => ReleaseHandle();

    public void Write(ReadOnlySpan<byte> pointerBuffer)
    {
        if (pointerBuffer.Length != Pointer.Size)
            throw new ArgumentException("Pointer buffer must be exactly 12 bytes.", nameof(pointerBuffer));

        EnsureOpen();
        RandomAccess.Write(_handle!, pointerBuffer, 0);
        Durability.FlushPointerIfNeeded(_handle!);
        if (!OperatingSystem.IsLinux())
            ReleaseHandle();
    }

    private void EnsureOpen()
    {
        if (_handle?.IsInvalid is false)
            return;

        // An invalid-but-non-null handle must be fully released before reopening, otherwise the stale
        // SafeFileHandle leaks.
        ReleaseHandle();

        var options = Durability.GetPointerFileOptions();
        _handle = File.OpenHandle(_currentPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete, options);
        if (RandomAccess.GetLength(_handle) != Pointer.Size)
            RandomAccess.SetLength(_handle, Pointer.Size);
    }

    private void ReleaseHandle()
    {
        _handle?.Dispose();
        _handle = null;
    }
}
