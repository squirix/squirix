using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Node-owned endpoint execution services consumed by transport adapters through runtime contracts.</summary>
internal static class NodeEndpointServiceRegistration
{
    extension(IServiceCollection services)
    {
        /// <summary>Registers inbound endpoint cache routing used by REST and gRPC adapters.</summary>
        /// <param name="persistenceEnabled">When true, registers durable health-ready detail providers.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal IServiceCollection AddSquirixNodeEndpointServices(bool persistenceEnabled = false)
        {
            _ = services.AddSingleton<IInboundEndpointCacheOperations<object?>, InboundEndpointCacheOperations<object?>>();
            _ = persistenceEnabled ? services.AddSingleton<IHealthReadyDetailsProvider>(static sp => new HealthReadyDetailsProvider(
                    new HealthReadyDependencies(
                        sp.GetRequiredService<ManifestStore>(),
                        sp.GetRequiredService<IRetentionCleanupReadinessStatus>(),
                        sp.GetRequiredService<IJournalCoordinator>(),
                        sp.GetRequiredService<Coordinator>(),
                        sp.GetRequiredService<IJournalCompactionStatus>(),
                        sp.GetRequiredService<ClusterConfig>(),
                        sp.GetRequiredService<IMemoryUsageAccounting>()),
                    sp.GetRequiredService<IMemoryPressureStateEvaluator>(),
                    sp.GetRequiredService<PressureOptions>()))
                : services.AddSingleton<IHealthReadyDetailsProvider, EphemeralHealthReadyDetailsProvider>();

            return services;
        }
    }
}
