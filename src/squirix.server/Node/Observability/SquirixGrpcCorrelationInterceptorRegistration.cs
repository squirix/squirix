using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Node.Observability;

internal static class SquirixGrpcCorrelationInterceptorRegistration
{
    public static IServiceCollection AddSquirixGrpcCorrelationInterceptor(this IServiceCollection services)
    {
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IConfigureOptions<GrpcServiceOptions>, CorrelationGrpcServiceOptionsConfigurator>());
        return services;
    }
}
