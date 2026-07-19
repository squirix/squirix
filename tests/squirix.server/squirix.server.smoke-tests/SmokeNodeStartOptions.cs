using System;
using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.Hosting;
using Xunit;

namespace Squirix.Server.SmokeTests;

/// <summary>Optional knobs for <see cref="SmokeTestBase" /> node startup.</summary>
internal sealed class SmokeNodeStartOptions
{
    internal Func<string, ServerCallPolicy>? CallPolicyFactory { get; init; }

    internal Action<GrpcServiceOptions>? ConfigureGrpc { get; init; }

    internal Action<IServiceCollection>? ServicesConfigure { get; init; }

    internal TriggerOptions? SnapshotOptions { get; init; }

    internal PersistenceOptions? PersistenceOptions { get; init; }

    internal bool UsePersistence { get; init; }

    internal ITestOutputHelper? Output { get; init; }

    internal bool CleanTestDir { get; init; } = true;

    internal string? ExtraScope { get; init; }

    internal TestNodeSecurityOptions? Security { get; init; }

    internal AdmissionOptions? BackpressureOptions { get; init; }

    internal PressureOptions? MemoryPressureOptions { get; init; }
}
