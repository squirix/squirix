using System;
using Grpc.AspNetCore.Server;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Adapters.Endpoint;

internal static class FrameworkServiceRegistration
{
    internal static IServiceCollection AddSquirixFrameworkServices(this IServiceCollection services, bool enableDetailedGrpcErrors, Action<GrpcServiceOptions>? configureGrpc)
    {
        _ = services.AddGrpc(o =>
        {
            o.EnableDetailedErrors = enableDetailedGrpcErrors;
            o.MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes;
            o.MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes;
            o.Interceptors.Add<InboundCallInterceptor>();
            configureGrpc?.Invoke(o);
        });
        _ = services.AddSingleton<MatcherPolicy>(static sp =>
        {
            var mtls = sp.GetRequiredService<MtlsCertificate>();
            var peers = mtls is { Enabled: true, PeerChains: { } peerChains }
                ? new VerifiedPeerCertificates(peerChains, MtlsTopology.GetRemotePeerNodeIds(sp.GetRequiredService<TopologyOptions>()), TimeProvider.System)
                : null;
            return new InternalListenerMatcherPolicy(peers);
        });
        _ = services.AddHealthChecks();
        _ = services.ConfigureHttpJsonOptions(static o => o.SerializerOptions.PropertyNameCaseInsensitive = true);
        _ = services.AddSingleton(static sp => new InboundCallInterceptor(
            sp.GetRequiredService<IRemoteInvocationScopeFactory>(),
            sp.GetRequiredService<TopologyOptions>(),
            sp.GetRequiredService<MtlsOptions>(),
            sp.GetRequiredService<MtlsCertificate>(),
            sp.GetRequiredService<RingAgreement>(),
            sp.GetRequiredService<ServerCallCorrelation>(),
            sp.GetRequiredService<ILogger<InboundCallInterceptor>>()));

        return services;
    }
}
