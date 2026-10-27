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

        // Clients never compress requests; refusing them rules out decompression bombs. Post-configure because
        // gRPC re-adds gzip and deflate after AddGrpc callbacks when the provider list is empty.
        _ = services.PostConfigure<GrpcServiceOptions>(static o => o.CompressionProviders.Clear());
        _ = services.AddSingleton<MatcherPolicy>(static sp => new InternalListenerMatcherPolicy(sp.GetService<VerifiedPeerCertificates>()));
        _ = services.AddHealthChecks();
        _ = services.ConfigureHttpJsonOptions(static o => o.SerializerOptions.PropertyNameCaseInsensitive = true);

        // One verifier is shared by every inbound consumer so an expiry on an open connection is logged once; it resolves to null (GetService) when mTLS is disabled.
        _ = services.AddSingleton(static sp => VerifiedPeerCertificates.CreateIfEnabled(
            sp.GetRequiredService<TopologyOptions>(),
            sp.GetRequiredService<MtlsCertificate>(),
            TimeProvider.System,
            sp.GetRequiredService<ILogger<VerifiedPeerCertificates>>())!);
        _ = services.AddSingleton(static sp => new InboundCallInterceptor(
            sp.GetRequiredService<IRemoteInvocationScopeFactory>(),
            sp.GetRequiredService<MtlsOptions>(),
            sp.GetService<VerifiedPeerCertificates>(),
            sp.GetRequiredService<RingAgreement>(),
            sp.GetRequiredService<ServerCallCorrelation>()));

        return services;
    }
}
