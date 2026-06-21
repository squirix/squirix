using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Reusable <c>man-current</c> handle for the journal roll hot path.</summary>
internal sealed class ManifestPersistentPointerWriter : IManifestPointerWriter
{
    private readonly string _currentPath;
    private SafeFileHandle? _handle;
    private int _unixFileDescriptor = -1;

    internal ManifestPersistentPointerWriter(string currentPath) => _currentPath = currentPath;

    public int UnixFileDescriptor
    {
        get
        {
            if (!OperatingSystem.IsLinux())
                return -1;

            EnsureOpen();
            return _unixFileDescriptor;
        }
    }

    public void Dispose() => ReleaseHandle();

    public void Write(ReadOnlySpan<byte> pointerBuffer)
    {
        if (pointerBuffer.Length != ManifestPointer.Size)
            throw new ArgumentException("Pointer buffer must be exactly 12 bytes.", nameof(pointerBuffer));

        EnsureOpen();
        RandomAccess.Write(_handle!, pointerBuffer, 0);
        ManifestDurability.FlushPointerIfNeeded(_handle!);
        if (!OperatingSystem.IsLinux())
            ReleaseHandle();
    }

    private void ReleaseHandle()
    {
        _handle?.Dispose();
        _handle = null;
        _unixFileDescriptor = -1;
    }

    [SuppressMessage("Security", "S3869:SafeHandle.DangerousGetHandle should not be called", Justification = "Linux io_uring batch durability requires the raw file descriptor for the persistent pointer handle.")]
    private void EnsureOpen()
    {
        if (_handle is not null && !_handle.IsInvalid)
            return;

        var options = ManifestDurability.GetPointerFileOptions();
        _handle = File.OpenHandle(_currentPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete, options);
        if (RandomAccess.GetLength(_handle) != ManifestPointer.Size)
            RandomAccess.SetLength(_handle, ManifestPointer.Size);

        if (OperatingSystem.IsLinux())
            _unixFileDescriptor = _handle.DangerousGetHandle().ToInt32();
    }
}
