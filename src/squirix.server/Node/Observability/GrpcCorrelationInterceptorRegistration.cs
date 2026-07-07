using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Node.Observability;

internal static class GrpcCorrelationInterceptorRegistration
{
    internal static IServiceCollection AddSquirixGrpcCorrelationInterceptor(this IServiceCollection services)
    {
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IConfigureOptions<GrpcServiceOptions>, GrpcCorrelationOptionsConfigurator>());
        return services;
    }
}
