using System.Net;
using System.Net.Sockets;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Helpers for resolving local host network endpoints in tests.</summary>
public static class LocalHostNetworking
{
    /// <summary>Returns the first non-loopback IPv4 address reported for the local host name, if any.</summary>
    /// <returns>An IPv4 dotted-quad string, or <see langword="null" /> when none is available.</returns>
    public static string? TryGetLocalNonLoopbackIpv4()
    {
        var addresses = Dns.GetHostAddresses(Dns.GetHostName());
        for (var i = 0; i < addresses.Length; i++)
        {
            var address = addresses[i];
            if (address.AddressFamily is not AddressFamily.InterNetwork)
                continue;

            if (IPAddress.IsLoopback(address))
                continue;

            return address.ToString();
        }

        return null;
    }
}
