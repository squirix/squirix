using System;
using System.Collections.Generic;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Mtls;

internal static class MtlsInternalPortPool
{
    private static readonly PortAllocator Allocator = new(
        HostPortRegions.StartInclusive(HostPortRegion.MtlsInternal),
        HostPortRegions.EndExclusive(HostPortRegion.MtlsInternal) - 1);

    /// <summary>Allocates a dedicated internal listener port that differs from all excluded primary ports.</summary>
    /// <param name="excludedPorts">Primary listener ports that must not be reused for internal mTLS.</param>
    /// <returns>An internal listener port for cluster mTLS.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="excludedPorts" /> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown if no internal listener port can be allocated within the attempt budget.</exception>
    public static int AllocateInternalPort(HashSet<int> excludedPorts)
    {
        ArgumentNullException.ThrowIfNull(excludedPorts);

        for (var attempt = 0; attempt < 64; attempt++)
        {
            var port = Allocator.Allocate();
            var isExcluded = false;
            foreach (var excludedPort in excludedPorts)
            {
                if (excludedPort != port)
                    continue;
                isExcluded = true;
                break;
            }

            if (!isExcluded)
                return port;
        }

        throw new InvalidOperationException("Failed to allocate a cluster mTLS internal listener port for tests.");
    }
}
