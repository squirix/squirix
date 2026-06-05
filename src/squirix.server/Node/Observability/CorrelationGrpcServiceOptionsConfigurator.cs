using Grpc.AspNetCore.Server;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Registers the correlation server interceptor after adapter-owned gRPC interceptors are configured.
/// </summary>
internal sealed class CorrelationGrpcServiceOptionsConfigurator : IConfigureOptions<GrpcServiceOptions>
{
    public void Configure(GrpcServiceOptions options) => options.Interceptors.Add<Correlation.ServerInterceptor>();
}
