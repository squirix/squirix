using System;
using System.Net.Http;
using Grpc.Core.Interceptors;

namespace Squirix.Server.Cluster.Transport;

internal sealed class ServerClientPoolArgs
{
    internal bool InterNodeMtlsEnabled { get; init; }

    internal Interceptor? Interceptor { get; init; }

    internal Interceptor? InternalOwnerInterceptor { get; init; }

    internal MtlsCertificateMaterial? MtlsMaterial { get; init; }

    internal MtlsOptions? MtlsOptions { get; init; }

    internal Func<string, HttpMessageHandler>? PeerHandlerFactory { get; init; }

    internal required Func<string, IServerCallPolicy> PolicyFactory { get; init; }
}
