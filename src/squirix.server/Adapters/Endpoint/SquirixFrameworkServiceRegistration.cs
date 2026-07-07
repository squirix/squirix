using System;
using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Limits;

namespace Squirix.Server.Adapters.Endpoint;

internal static class SquirixFrameworkServiceRegistration
{
    internal static IServiceCollection AddSquirixFrameworkServices(this IServiceCollection services, bool enableDetailedGrpcErrors, Action<GrpcServiceOptions>? configureGrpc)
    {
        _ = services.AddGrpc(o =>
        {
            o.EnableDetailedErrors = enableDetailedGrpcErrors;
            o.MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes;
            o.MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes;
            o.Interceptors.Add<ResourceExhaustedExceptionInterceptor>();
            o.Interceptors.Add<InvocationContextInterceptor>();
            configureGrpc?.Invoke(o);
        });
        _ = services.AddHealthChecks();
        _ = services.ConfigureHttpJsonOptions(static o => o.SerializerOptions.PropertyNameCaseInsensitive = true);
        _ = services.AddSingleton<InvocationContextInterceptor>();
        _ = services.AddSingleton<ResourceExhaustedExceptionInterceptor>();

        return services;
    }
}
