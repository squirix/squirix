using System;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Adapters.Endpoint;

internal static class FrameworkServiceRegistration
{
    internal static IServiceCollection AddSquirixFrameworkServices(this IServiceCollection services)
    {
        _ = services.AddSingleton<CacheRequestAdmission>();
        _ = services.AddSingleton<BinaryDispatch>();

        _ = services.AddSingleton<MatcherPolicy>(static sp => new InternalListenerMatcherPolicy(sp.GetService<VerifiedPeerCertificates>()));
        _ = services.AddHealthChecks();
        _ = services.ConfigureHttpJsonOptions(static o => o.SerializerOptions.PropertyNameCaseInsensitive = true);

        // One verifier is shared by every inbound consumer so an expiry on an open connection is logged once; it resolves to null (GetService) when mTLS is disabled.
        _ = services.AddSingleton(static sp => VerifiedPeerCertificates.CreateIfEnabled(
            sp.GetRequiredService<TopologyOptions>(),
            sp.GetRequiredService<MtlsCertificate>(),
            TimeProvider.System,
            sp.GetRequiredService<ILogger<VerifiedPeerCertificates>>())!);

        return services;
    }
}
