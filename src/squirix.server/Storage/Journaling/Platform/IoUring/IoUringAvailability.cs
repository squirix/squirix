using System;
using System.IO;
using System.Threading;

namespace Squirix.Server.Storage.Journaling.Platform.IoUring;

/// <summary>Probes whether the host kernel exposes a usable io_uring ring.</summary>
internal static class IoUringAvailability
{
    private static readonly Lazy<bool> Supported = new(Probe, LazyThreadSafetyMode.ExecutionAndPublication);

    internal static bool IsSupported => OperatingSystem.IsLinux() && Supported.Value;

    private static bool Probe()
    {
        if (!OperatingSystem.IsLinux())
            return false;

        try
        {
            using var ring = new IoUringJournalRing(4);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
        catch (PlatformNotSupportedException)
        {
            return false;
        }
    }
}
