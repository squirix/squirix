using System;
using Grpc.AspNetCore.Server;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Optional knobs for <see cref="BlackBoxCluster" /> node startup, expressed without server-internal option types.</summary>
[Immutable]
internal sealed class BlackBoxStartOptions : ClusterStartOptions
{
    /// <summary>Gets optional admission-control overrides; <see langword="null" /> keeps the server defaults.</summary>
    internal TestNodeBackpressureOptions? BackpressureOptions { get; init; }

    /// <summary>Gets an optional gRPC server configuration hook (for example extra interceptors).</summary>
    internal Action<GrpcServiceOptions>? ConfigureGrpc { get; init; }

    /// <summary>Gets optional memory pressure overrides; <see langword="null" /> keeps the server defaults.</summary>
    internal TestNodeMemoryPressureOptions? MemoryPressureOptions { get; init; }
}
