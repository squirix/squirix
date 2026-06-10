using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Endpoint;

/// <summary>
/// Node-owned endpoint execution services consumed by transport adapters through runtime contracts.
/// </summary>
internal static class SquirixNodeEndpointServiceRegistration
{
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Registers inbound endpoint cache routing used by REST and gRPC adapters.
        /// </summary>
        public IServiceCollection AddSquirixNodeEndpointServices()
        {
            _ = services.AddSingleton<IInboundEndpointCacheOperations<object?>, InboundEndpointCacheOperations<object?>>();
            _ = services.AddSingleton<IHealthReadyDetailsProvider, HealthReadyDetailsProvider>();
            return services;
        }
    }
}
