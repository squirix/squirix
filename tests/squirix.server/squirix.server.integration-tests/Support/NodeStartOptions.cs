using System;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Optional knobs for <see cref="NodeIntegrationTestBase" /> node startup.</summary>
internal sealed class NodeStartOptions
{
    internal bool CleanTestDir { get; init; } = true;

    internal ulong ConfigurationGeneration { get; init; } = 1;

    internal string? ExtraScope { get; init; }

    internal PersistenceOptions? PersistenceOptions { get; init; }

    internal int ReplicaCount { get; init; } = 1;

    internal TestNodeSecurityOptions? Security { get; init; }

    internal Action<IServiceCollection>? ServicesConfigure { get; init; }

    internal bool UsePersistence { get; init; }

    internal bool WaitForRecovery { get; init; } = true;
}
