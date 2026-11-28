using System;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.Logging;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Adapters.Endpoint;

internal static class FrameworkServiceRegistration
{
    internal static NodeServices AddSquirixFrameworkServices(this NodeServices services)
    {
        _ = services.Own(static sp => new CacheRequestAdmission(
            sp.Require<IBackpressureGate>(),
            sp.Require<IBackpressureClientIdResolver>()));
        _ = services.Own(static sp => new BinaryDispatch(
            sp.Require<RoutedCacheOperations<StoredValue>>(),
            sp.Require<CacheRequestAdmission>(),
            sp.Require<ServerCallCorrelation>(),
            sp.Find<BinaryCallObserver>()));

        _ = services.Own(static sp => new ReadinessMonitor(
            sp.All<ReadinessCheck>(),
            sp,
            sp.Require<IServerLoggerFactory>().CreateLogger<ReadinessMonitor>()));

        // One verifier is shared by every inbound consumer so an expiry on an open connection is logged once; it resolves to null (GetService) when mTLS is disabled.
        _ = services.Own(static sp => VerifiedPeerCertificates.CreateIfEnabled(
            sp.Require<TopologyOptions>(),
            sp.Require<MtlsCertificate>(),
            TimeProvider.System,
            sp.Require<IServerLoggerFactory>().CreateLogger<VerifiedPeerCertificates>())!);

        return services;
    }
}
