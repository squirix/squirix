using System;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>Optional knobs for <see cref="NodeIntegrationTestBase" /> node startup.</summary>
[Immutable]
internal sealed class IntegrationStartOptions : ClusterStartOptions
{
    internal bool CleanTestDir { get; init; } = true;

    internal string? ExtraScope { get; init; }

    internal bool FoundationOnly { get; init; }

    /// <summary>
    /// Gets a value indicating whether the node starts without provisioned cluster mTLS: peers carry no internode
    /// URLs and the host receives empty <see cref="Squirix.Server.Cluster.MtlsOptions" /> instead of test-generated
    /// material. Only for negative tests of the RF&gt;1 mTLS activation guard.
    /// </summary>
    internal bool OmitClusterMtls { get; init; }

    internal PersistenceOptions? PersistenceOptions { get; init; }

    internal Action<IServiceCollection>? ServicesConfigure { get; init; }

    internal bool UsePersistence { get; init; }

    internal bool WaitForRecovery { get; init; } = true;
}
