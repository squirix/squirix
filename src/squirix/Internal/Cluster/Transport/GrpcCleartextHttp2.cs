using System;
using System.Net.Http;

namespace Squirix.Internal.Cluster.Transport;

internal static class GrpcCleartextHttp2
{
    public static void EnableIfNeeded(string url)
    {
        if (!url.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            return;

        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_HTTP_SOCKETSHTTPHANDLER_HTTP2UNENCRYPTEDSUPPORT", "true");
    }

    public static HttpMessageHandler CreateChannelHandler(string url)
    {
        if (!url.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            return new SocketsHttpHandler();

        EnableIfNeeded(url);
        return new SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true,
            UseProxy = false,
        };
    }
}
