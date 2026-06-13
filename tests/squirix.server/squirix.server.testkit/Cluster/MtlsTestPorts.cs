using System;
using System.Collections.Generic;
using Squirix.Server.TestKit.Http;

namespace Squirix.Server.TestKit.Cluster;

internal static class MtlsTestPorts
{
    private static readonly PortAllocator Allocator = new(52000, 55999);

    /// <summary>
    /// Allocates a dedicated internal listener port that differs from all excluded primary ports.
    /// </summary>
    /// <param name="excludedPorts">Primary listener ports that must not be reused for internal mTLS.</param>
    /// <returns>An internal listener port for cluster mTLS.</returns>
    public static int AllocateInternalPort(IReadOnlyCollection<int> excludedPorts)
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
