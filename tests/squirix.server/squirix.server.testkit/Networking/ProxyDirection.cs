namespace Squirix.Server.TestKit.Networking;

/// <summary>The direction bytes travel through a <see cref="TcpPartitionProxy" />.</summary>
public enum ProxyDirection
{
    /// <summary>Bytes the connecting client sends towards the upstream listener.</summary>
    ClientToUpstream = 0,

    /// <summary>Bytes the upstream listener sends back towards the connecting client.</summary>
    UpstreamToClient = 1,
}
