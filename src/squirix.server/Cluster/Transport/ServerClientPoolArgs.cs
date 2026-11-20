using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

[Immutable]
internal sealed record ServerClientPoolArgs
{
    internal MtlsCertificate? Certificate { get; init; }

    /// <summary>
    /// Gets the longest time the dial of a forward channel, before any TLS handshake, may take; <see cref="TopologyOptions.DefaultForwardConnectTimeout" />
    /// when <see langword="null" />. The lease channel gets no dial bound; both keep a long bound of the whole connect.
    /// </summary>
    internal TimeSpan? ForwardConnectTimeout { get; init; }

    internal Func<string, Uri, PeerHandshakeVerifier?, TrackedConnections, TimeSpan, PeerChannels>? ChannelsOverride { get; init; }

    internal bool InterNodeMtlsEnabled { get; init; }

    internal string? RingFingerprint { get; init; }

    internal MtlsOptions? MtlsOptions { get; init; }

    internal Func<string, PeerSocketOptions>? SocketOptions { get; init; }

    internal required Func<string, IServerCallPolicy> PolicyFactory { get; init; }

    /// <summary>Gets how long disposal waits for the peer policies to drain; ten seconds when <see langword="null" />.</summary>
    internal TimeSpan? ShutdownBudget { get; init; }

    /// <summary>Gets the clock that measures the shutdown budget.</summary>
    internal required TimeProvider TimeProvider { get; init; }
}
