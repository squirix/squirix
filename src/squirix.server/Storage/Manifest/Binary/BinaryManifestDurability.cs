using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Manifest.Binary;

/// <summary>WAL-ordered durable writes for binary manifest data files and the fixed-size CURRENT pointer.</summary>
internal static class BinaryManifestDurability
{
    /// <summary>Writes a numbered manifest file without write-through; flushes to disk before returning.</summary>
    /// <param name="targetPath">Path to the new <c>.bmqx</c> file.</param>
    /// <param name="encoded">Encoded manifest bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when manifest bytes are on stable storage.</returns>
    internal static Task WriteManifestDataFileAsync(string targetPath, ReadOnlyMemory<byte> encoded, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        WriteManifestDataFileBlocking(targetPath, encoded.Span);
        return Task.CompletedTask;
    }

    /// <summary>Overwrites the fixed-size SQMC pointer in place with write-through on Windows.</summary>
    /// <param name="currentPath">Path to <c>man-current</c>.</param>
    /// <param name="pointerBuffer">Exactly 12 encoded SQMC bytes.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the pointer is durable.</returns>
    internal static Task WriteCurrentPointerAsync(string currentPath, ReadOnlyMemory<byte> pointerBuffer, CancellationToken cancellationToken)
    {
        if (pointerBuffer.Length != BinaryManifestPointer.Size)
            throw new ArgumentException("Pointer buffer must be exactly 12 bytes.", nameof(pointerBuffer));

        cancellationToken.ThrowIfCancellationRequested();
        WriteCurrentPointerBlocking(currentPath, pointerBuffer.Span);
        return Task.CompletedTask;
    }

    private static void WriteManifestDataFileBlocking(string targetPath, ReadOnlySpan<byte> encoded)
    {
        using var handle = File.OpenHandle(targetPath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        RandomAccess.Write(handle, encoded, 0);
        RandomAccess.FlushToDisk(handle);
    }

    private static void WriteCurrentPointerBlocking(string currentPath, ReadOnlySpan<byte> pointerBuffer)
    {
        var options = OperatingSystem.IsWindows() ? FileOptions.WriteThrough : FileOptions.None;
        using var handle = File.OpenHandle(currentPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, options);
        RandomAccess.SetLength(handle, BinaryManifestPointer.Size);
        RandomAccess.Write(handle, pointerBuffer, 0);
        if (!OperatingSystem.IsWindows())
            RandomAccess.FlushToDisk(handle);
    }
}
