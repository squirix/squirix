using System;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Transport-owned component registrations for native peer client pooling.</summary>
internal static class ServiceRegistration
{
    extension(NodeServices services)
    {
        /// <summary>Registers internode client pool and ownership interceptors.</summary>
        /// <param name="cluster">Cluster topology configuration.</param>
        /// <param name="callPolicyFactory">Optional per-endpoint call policy factory.</param>
        /// <param name="maxInFlight">Entry node maximum in-flight operations that sizes the per-owner forwarding permits.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal NodeServices AddSquirixClusterTransport(
            TopologyOptions cluster,
            Func<string, ServerCallPolicy>? callPolicyFactory,
            int maxInFlight)
        {
            _ = services.Own(sp => new ServerCallCorrelation(sp.Require<ILoggerFactory>().CreateLogger<ServerCallCorrelation>(), cluster.NodeId));

            _ = services.Own<IServerClientPool>(sp =>
            {
                var certificate = sp.Require<MtlsCertificate>();
                var mtlsOptions = sp.Require<MtlsOptions>();
                var interNodeMtlsEnabled = certificate.Enabled;
                return new ServerClientPool(
                    CopyPeers(cluster),
                    new ServerClientPoolArgs
                    {
                        PolicyFactory = callPolicyFactory ?? (peer => ForwardingCallPolicyDefaults.Create(
                            sp.Require<ServerCallPolicyInstrumentation>(),
                            peer,
                            maxInFlight,
                            sp.Require<TimeProvider>())),

                        // The shutdown budget runs on the system clock, so a host clock that never advances cannot park a dispose.
                        TimeProvider = TimeProvider.System,
                        ForwardConnectTimeout = cluster.ForwardConnectTimeout,
                        RingFingerprint = sp.Require<RingFingerprint>().Value,
                        MtlsOptions = mtlsOptions,
                        Certificate = certificate,
                        InterNodeMtlsEnabled = interNodeMtlsEnabled,
                        SocketOptions = sp.Find<Func<string, PeerSocketOptions>>(),
                    },
                    sp.Require<ServerClientPoolMetrics>(),
                    sp.Require<ILoggerFactory>().CreateLogger<ServerClientPool>(),
                    sp.Require<ILoggerFactory>().CreateLogger<PeerHandshakeVerifier>());
            });

            return services;
        }
    }

    private static ServerPeer[] CopyPeers(TopologyOptions cluster)
    {
        var peers = cluster.Peers;
        var copy = new ServerPeer[peers.Count];

        for (var i = 0; i < peers.Count; i++)
            copy[i] = peers[i];

        return copy;
    }
}
