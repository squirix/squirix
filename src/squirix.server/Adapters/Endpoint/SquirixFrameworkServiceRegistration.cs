using System;
using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint.Grpc;
using Squirix.Server.Serialization;

namespace Squirix.Server.Adapters.Endpoint;

internal static class SquirixFrameworkServiceRegistration
{
    public static IServiceCollection AddSquirixFrameworkServices(this IServiceCollection services, Action<GrpcServiceOptions>? configureGrpc)
    {
        _ = services.AddGrpc(o =>
        {
            o.EnableDetailedErrors = true;
            o.Interceptors.Add<ResourceExhaustedExceptionInterceptor>();
            o.Interceptors.Add<GrpcInvocationContextInterceptor>();
            configureGrpc?.Invoke(o);
        });
        _ = services.AddHealthChecks();
        _ = services.ConfigureHttpJsonOptions(static o =>
        {
            o.SerializerOptions.PropertyNameCaseInsensitive = true;
            o.SerializerOptions.Converters.Add(new StoredJsonPayloadConverter());
        });
        _ = services.AddSingleton<GrpcInvocationContextInterceptor>();
        _ = services.AddSingleton<ResourceExhaustedExceptionInterceptor>();

        return services;
    }
}
