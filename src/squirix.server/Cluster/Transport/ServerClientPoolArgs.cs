using System;
using System.Net.Http;
using Grpc.Core.Interceptors;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

[Immutable]
internal sealed class ServerClientPoolArgs
{
    internal MtlsCertificate? Certificate { get; init; }

    /// <summary>
    /// Gets the longest time the dial of a forward channel, before any TLS handshake, may take; <see cref="TopologyOptions.DefaultForwardConnectTimeout" />
    /// when <see langword="null" />. The lease channel gets no dial bound; both keep a long bound of the whole connect.
    /// </summary>
    internal TimeSpan? ForwardConnectTimeout { get; init; }

    internal bool InterNodeMtlsEnabled { get; init; }

    internal Interceptor? Interceptor { get; init; }

    internal Interceptor? InternalOwnerInterceptor { get; init; }

    internal MtlsOptions? MtlsOptions { get; init; }

    /// <summary>
    /// Gets an optional replacement for the handlers the pool creates and owns itself, given the mTLS material
    /// (<see langword="null" /> for plain HTTPS), the peer node id and the pool's connection gate (test seam for handler ownership).
    /// </summary>
    internal Func<MtlsCertificate?, string, TrackedConnections, HttpMessageHandler>? OwnedHandlerFactory { get; init; }

    /// <summary>Gets an optional per-peer mTLS handler factory; the handlers it returns stay owned by its caller and are not disposed with the pool, but a <see cref="System.Net.Http.SocketsHttpHandler" /> among them has its connections tracked and aborted on disposal. The factory must return a fresh, unstarted handler per call: the pool replaces the handler's connect callback, and a handler that already belongs to a pool is rejected.</summary>
    internal Func<string, HttpMessageHandler>? PeerHandlerFactory { get; init; }

    internal required Func<string, IServerCallPolicy> PolicyFactory { get; init; }

    /// <summary>Gets how long disposal waits for the peer policies to drain; ten seconds when <see langword="null" />.</summary>
    internal TimeSpan? ShutdownBudget { get; init; }

    /// <summary>Gets the clock that measures the shutdown budget; <see cref="System.TimeProvider.System" /> when <see langword="null" />.</summary>
    internal TimeProvider? TimeProvider { get; init; }
}
