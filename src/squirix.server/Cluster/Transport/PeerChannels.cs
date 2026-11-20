using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;
using Squirix.Transport.Binary;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Owns separate channels for forwarded client operations and internode calls.</summary>
[Immutable]
internal sealed class PeerChannels
{
    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);
    private static readonly PeerChannels Disabled = new(UnavailablePeerClient.Instance, UnavailablePeerClient.Instance, null, null);

    private readonly IDisposable? _forward;
    private readonly IDisposable? _replication;

    internal PeerChannels(IPeerClient forward, IPeerClient replication, IDisposable? forwardOwner, IDisposable? replicationOwner)
    {
        Forward = forward;
        Replication = replication;
        _forward = forwardOwner;
        _replication = replicationOwner;
    }

    internal IPeerClient Forward { get; }
    internal IPeerClient Replication { get; }

    internal static PeerChannels Create(
        string nodeId,
        Uri address,
        ServerClientPoolArgs args,
        PeerHandshakeVerifier? verifier,
        TrackedConnections connections,
        TimeSpan forwardConnectTimeout)
    {
        if (!args.InterNodeMtlsEnabled)
            return Disabled;

        if (args.Certificate is not { Enabled: true } material || verifier == null)
            throw new InvalidOperationException("Cluster mTLS material must be loaded for internode transport.");

        var options = args.SocketOptions?.Invoke(nodeId);
        var forward = new NativePeerClient((address, nodeId), material, verifier, connections, forwardConnectTimeout, args.RingFingerprint, options);
        try
        {
            var replication = new NativePeerClient((address, nodeId), material, verifier, connections, ConnectTimeout, null, options);
            return new PeerChannels(forward, replication, forward, replication);
        }
        catch
        {
            forward.Dispose();
            throw;
        }
    }

    internal Exception? Close()
    {
        var forward = Isolated.Run(_forward, static channel => channel?.Dispose());
        var replication = Isolated.Run(_replication, static channel => channel?.Dispose());
        return forward ?? replication;
    }

    [Immutable]
    private sealed class UnavailablePeerClient : IPeerClient
    {
        internal static readonly UnavailablePeerClient Instance = new();

        public Task<T> SendAsync<T>(string path, IMessage request, MessageParser<T> parser, CancellationToken cancellationToken)
            where T : IMessage<T> => Task.FromException<T>(new ServerCallException(FrameStatus.Unavailable, "Peer transport is unavailable without cluster mTLS."));
    }
}
