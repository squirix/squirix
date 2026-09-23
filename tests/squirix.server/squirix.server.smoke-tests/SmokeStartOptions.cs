using System;
using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.SmokeTests;

/// <summary>Optional knobs for <see cref="SmokeTestBase" /> node startup.</summary>
[Immutable]
internal sealed class SmokeStartOptions : ClusterStartOptions
{
    internal AdmissionOptions? BackpressureOptions { get; init; }

    internal Action<GrpcServiceOptions>? ConfigureGrpc { get; init; }

    internal PressureOptions? MemoryPressureOptions { get; init; }

    internal Action<IServiceCollection>? ServicesConfigure { get; init; }
}
