using System;
using System.IO;
using System.Runtime.Versioning;
using System.Threading;
using Squirix.Server.Storage.Journaling.Platform.IoUring;

namespace Squirix.Server.Storage.Manifest;

/// <summary>
/// Per-store io_uring writer for manifest roll durability. Reuses a single ring across rolls instead of
/// allocating <c>new IoUringJournalRing(32)</c> on every roll (audit item P3). Rolls are serialized on
/// the manifest-roll thread; a lock additionally guards the direct (test) invocation path and ring
/// reset on failure.
/// </summary>
internal sealed class ManifestIoUringRollWriter : IDisposable
{
    private const uint RingEntries = 32;
    private readonly Lock _sync = new();
    private IoUringJournalRing? _ring;
    private bool _disposed;

    private static bool IsSupported => OperatingSystem.IsLinux() && IoUringAvailability.IsSupported;

    public void Dispose()
    {
        lock (_sync)
        {
            _disposed = true;

            // _ring is only ever constructed on the Linux io_uring path, so this is unreachable on other
            // platforms; the OS guard keeps the platform analyzer satisfied.
            if (_ring is not null && OperatingSystem.IsLinux())
                _ring.Dispose();

            _ring = null;
        }
    }

    /// <summary>Attempts a durable io_uring manifest roll; returns false when io_uring is unavailable.</summary>
    /// <param name="targetPath">Path to the new numbered manifest data file.</param>
    /// <param name="encoded">Encoded manifest bytes.</param>
    /// <param name="pointerWriter">Reusable pointer writer exposing the <c>man-current</c> descriptor.</param>
    /// <param name="pointerBuffer">Exactly 12 encoded SQMC pointer bytes.</param>
    /// <returns>True when the roll was durably written via io_uring; otherwise false.</returns>
    internal bool TryWriteRollBlocking(string targetPath, ReadOnlySpan<byte> encoded, IManifestPointerWriter pointerWriter, ReadOnlySpan<byte> pointerBuffer)
    {
        if (!OperatingSystem.IsLinux() || !IsSupported)
            return false;

        return TryWriteRollBlockingLinux(targetPath, encoded, pointerWriter, pointerBuffer);
    }

    private static void TryDeletePartialDataFile(string targetPath)
    {
        try
        {
            File.Delete(targetPath);
        }
        catch (IOException)
        {
            // Best effort: the fallback writer surfaces any persistent failure.
        }
        catch (UnauthorizedAccessException)
        {
            // Best effort: the fallback writer surfaces any persistent failure.
        }
    }

    [SupportedOSPlatform("linux")]
    private bool TryWriteRollBlockingLinux(string targetPath, ReadOnlySpan<byte> encoded, IManifestPointerWriter pointerWriter, ReadOnlySpan<byte> pointerBuffer)
    {
        var pointerFd = pointerWriter.UnixFileDescriptor;
        if (pointerFd < 0)
            return false;

        lock (_sync)
        {
            if (_disposed)
                return false;

            var dataFd = LinuxManifestFile.CreateNew(targetPath);
            try
            {
                LinuxManifestFile.Preallocate(dataFd, encoded.Length);
                var ring = _ring ??= new IoUringJournalRing(RingEntries);
                ring.WriteManifestRoll(dataFd, encoded, pointerFd, pointerBuffer);
                return true;
            }
            catch (IOException)
            {
                // Drop the ring on failure so the next roll rebuilds a clean one.
                _ring?.Dispose();
                _ring = null;

                // Remove the data file we just created so the portable fallback (FileMode.CreateNew)
                // does not trip over a pre-existing target.
                LinuxManifestFile.Close(dataFd);
                dataFd = -1;
                TryDeletePartialDataFile(targetPath);
                return false;
            }
            finally
            {
                LinuxManifestFile.Close(dataFd);
            }
        }
    }
}
