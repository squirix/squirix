using System;
using System.IO;
using System.Runtime.Versioning;
using Squirix.Server.Storage.Journaling.Platform.IoUring;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Cross-platform entry that delegates to io_uring on Linux when available.</summary>
internal static class ManifestIoUringRollDurability
{
    private static bool IsSupported => OperatingSystem.IsLinux() && IoUringAvailability.IsSupported;

    internal static bool TryWriteRollBlocking(
        string targetPath,
        ReadOnlySpan<byte> encoded,
        IManifestPointerWriter pointerWriter,
        ReadOnlySpan<byte> pointerBuffer)
    {
        if (!OperatingSystem.IsLinux())
            return false;

        return TryWriteRollBlockingLinux(targetPath, encoded, pointerWriter, pointerBuffer);
    }

    [SupportedOSPlatform("linux")]
    private static bool TryWriteRollBlockingLinux(
        string targetPath,
        ReadOnlySpan<byte> encoded,
        IManifestPointerWriter pointerWriter,
        ReadOnlySpan<byte> pointerBuffer)
    {
        if (!IsSupported)
            return false;

        var pointerFd = pointerWriter.UnixFileDescriptor;
        if (pointerFd < 0)
            return false;

        var dataFd = LinuxManifestFile.CreateNew(targetPath);
        try
        {
            LinuxManifestFile.Preallocate(dataFd, encoded.Length);
            using var ring = new IoUringJournalRing(32);
            ring.WriteManifestRoll(dataFd, encoded, pointerFd, pointerBuffer);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
        finally
        {
            LinuxManifestFile.Close(dataFd);
        }
    }
}
