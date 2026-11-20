using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Adapters.Endpoint;

internal static class FrameworkServiceRegistration
{
    internal static IServiceCollection AddSquirixFrameworkServices(this IServiceCollection services)
    {
        _ = services.AddSingleton(static sp => new CacheRequestAdmission(
            sp.GetRequiredService<IBackpressureGate>(),
            sp.GetRequiredService<IBackpressureClientIdResolver>()));
        _ = services.AddSingleton(static sp => new BinaryDispatch(
            sp.GetRequiredService<RoutedCacheOperations<object?>>(),
            sp.GetRequiredService<CacheRequestAdmission>(),
            sp.GetRequiredService<ServerCallCorrelation>(),
            sp.GetRequiredService<IRemoteInvocationScopeFactory>(),
            sp.GetService<BinaryCallObserver>()));

        _ = services.AddHealthChecks();

        // One verifier is shared by every inbound consumer so an expiry on an open connection is logged once; it resolves to null (GetService) when mTLS is disabled.
        _ = services.AddSingleton(static sp => VerifiedPeerCertificates.CreateIfEnabled(
            sp.GetRequiredService<TopologyOptions>(),
            sp.GetRequiredService<MtlsCertificate>(),
            TimeProvider.System,
            sp.GetRequiredService<ILogger<VerifiedPeerCertificates>>())!);

        return services;
    }
}
