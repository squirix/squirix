using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Node-owned endpoint execution services consumed by transport adapters through runtime contracts.</summary>
internal static class SquirixNodeEndpointServiceRegistration
{
    extension(IServiceCollection services)
    {
        /// <summary>Registers inbound endpoint cache routing used by REST and gRPC adapters.</summary>
        /// <param name="persistenceEnabled">When true, registers durable health-ready detail providers.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        public IServiceCollection AddSquirixNodeEndpointServices(bool persistenceEnabled = false)
        {
            _ = services.AddSingleton<IInboundEndpointCacheOperations<object?>, InboundEndpointCacheOperations<object?>>();
            _ = persistenceEnabled ? services.AddSingleton<IHealthReadyDetailsProvider, HealthReadyDetailsProvider>()
                : services.AddSingleton<IHealthReadyDetailsProvider, EphemeralHealthReadyDetailsProvider>();

            return services;
        }
    }
}
