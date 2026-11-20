using System;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Mtls;

internal static class InterNodeProbe
{
    internal static async Task<bool> NegotiatesPeerProtocolAsync(Uri address, CancellationToken cancellationToken)
    {
        using var client = new TcpClient();
        await client.ConnectAsync(address.DnsSafeHost, address.Port, cancellationToken).ConfigureAwait(false);
        await using var stream = new SslStream(client.GetStream());
        var options = new SslClientAuthenticationOptions { TargetHost = address.DnsSafeHost, ApplicationProtocols = [PeerFrames.Protocol] };
        try
        {
            await stream.AuthenticateAsClientAsync(options, cancellationToken).ConfigureAwait(false);
            return stream.NegotiatedApplicationProtocol == PeerFrames.Protocol;
        }
        catch (Exception error) when (error is AuthenticationException or IOException)
        {
            return false;
        }
    }
}
