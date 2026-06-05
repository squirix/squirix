using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint.Rest;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>
/// DI registrations for endpoint-only adapter glue that does not execute cache or persistence runtime.
/// </summary>
internal static class SquirixAdapterEndpointServiceRegistration
{
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Registers REST admin audit and other endpoint glue singletons used by adapter mapping.
        /// </summary>
        public IServiceCollection AddSquirixAdapterEndpointServices()
        {
            _ = services.AddSingleton<AdminAuditSink>();
            return services;
        }
    }
}
