using System;
using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.SmokeTests;

/// <summary>Optional knobs for <see cref="SmokeTestBase" /> node startup.</summary>
internal sealed class SmokeNodeStartOptions
{
    internal Action<GrpcServiceOptions>? ConfigureGrpc { get; init; }

    internal Action<IServiceCollection>? ServicesConfigure { get; init; }

    internal TestNodeSecurityOptions? Security { get; init; }

    internal AdmissionOptions? BackpressureOptions { get; init; }

    internal PressureOptions? MemoryPressureOptions { get; init; }
}
