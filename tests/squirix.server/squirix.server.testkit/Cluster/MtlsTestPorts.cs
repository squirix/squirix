using System;
using Squirix.Server.TestKit.Http;

namespace Squirix.Server.TestKit.Cluster;

internal static class MtlsTestPorts
{
    private static readonly PortAllocator Allocator = new(52000, 55999);

    /// <summary>
    /// Allocates a dedicated internal listener port that differs from the primary HTTPS port.
    /// </summary>
    /// <param name="primaryListenPort">Primary external HTTPS listener port.</param>
    /// <returns>An internal listener port for cluster mTLS.</returns>
    public static int AllocateInternalPort(int primaryListenPort)
    {
        for (var attempt = 0; attempt < 32; attempt++)
        {
            var port = Allocator.Allocate();
            if (port != primaryListenPort)
                return port;
        }

        throw new InvalidOperationException("Failed to allocate a cluster mTLS internal listener port for tests.");
    }
}
