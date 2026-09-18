using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers the public server configuration loader.</summary>
[Immutable]
public sealed class ConfiguratorTests : IsolatedStorageTestBase
{
    /// <summary>Canonicalizes a safe data directory override to an absolute path.</summary>
    [Test]
    public async Task CommandLineCanonicalizesDataDir()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Uri = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Uri = new Uri("https://localhost:5001") },
            ],
        };

        Configurator.ApplyCommandLineOverrides(options, null, Dir, true);
        _ = await Assert.That(options.DataDirectory).IsEqualTo(Path.GetFullPath(Dir));
    }

    /// <summary>Command-line overrides enable the replication opt-in.</summary>
    [Test]
    public async Task CommandLineEnablesReplicationOptIn()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Uri = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Uri = new Uri("https://localhost:5001") },
            ],
        };

        Configurator.ApplyCommandLineOverrides(options, null, null, false, true);
        _ = await Assert.That(options.ReplicationEnabled).IsTrue();
    }

    /// <summary>Rejects command-line data directory overrides that contain parent-directory segments.</summary>
    [Test]
    public async Task CommandLineOverridesTraversalDataDir()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Uri = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Uri = new Uri("https://localhost:5001") },
            ],
        };

        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(options, static value => Configurator.ApplyCommandLineOverrides(value, null, "../data", true));
        _ = await Assert.That(ex.Message).Contains("'.' or '..'", StringComparison.Ordinal);
    }

    /// <summary>CopyOptions preserves replica placement fields.</summary>
    [Test]
    public async Task CopyOptionsCopiesReplicaSettings()
    {
        var source = new SquirixServerOptions
        {
            NodeId = "node-a",
            Uri = new Uri("https://localhost:5001"),
            ReplicaCount = 3,
            ReplicationEnabled = true,
            ConfigurationGeneration = 9,
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Uri = new Uri("https://localhost:5001") },
            ],
        };
        var target = new SquirixServerOptions();
        Configurator.CopyOptions(source, target);
        _ = await Assert.That(target.ReplicaCount).IsEqualTo(3);
        _ = await Assert.That(target.ReplicationEnabled).IsTrue();
        _ = await Assert.That(target.ConfigurationGeneration).IsEqualTo(9u);
    }

    /// <summary>Ensures invalid peer topology returns structured errors.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoadFromFileErrorsForInvalidPeers(CancellationToken cancellationToken)
    {
        const string json = """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Peers":[{"NodeId":"node-b","Uri":"https://localhost:5002"}]}}}""";
        var path = NodePathKit.Combine(Dir, "invalid.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);
        var (success, _, error) = await Configurator.LoadFromFileAsync(path, cancellationToken);
        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("local NodeId", StringComparison.Ordinal);
    }

    /// <summary>TryLoadFromFile reports a clear error when the settings file is missing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoadFromFileErrorsWhenFileMissing(CancellationToken cancellationToken)
    {
        var (success, _, error) = await Configurator.LoadFromFileAsync(Path.Join(Dir, "missing.json"), cancellationToken);
        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("does not exist", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Ensures cluster settings can be loaded from a settings file path.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoadFromFileReadsClusterSection(CancellationToken cancellationToken)
    {
        const string json =
            """{"Squirix":{"Cluster":{"ClusterId":"c1","NodeId":"node-a","Uri":"https://localhost:5001","VirtualNodes":128,"Peers":[{"NodeId":"node-a","Uri":"https://localhost:5001"}]}}}""";
        var path = NodePathKit.Combine(Dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);
        var options = await Configurator.LoadAsync(path, cancellationToken);
        _ = await Assert.That(options.NodeId).IsEqualTo("node-a");
        _ = await Assert.That(options.ClusterId).IsEqualTo("c1");
    }

    /// <summary>Rejects settings paths that contain parent-directory segments.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoadFromFileRejectsTraversalPath(CancellationToken cancellationToken)
    {
        var (success, _, error) = await Configurator.LoadFromFileAsync("../Squirix.settings.json", cancellationToken);
        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("'.' or '..'", StringComparison.Ordinal);
    }

    /// <summary>Public path helpers reject traversal segments.</summary>
    [Test]
    public void ResolveValidatedHelpersRejectTraversal()
    {
        _ = NodeExceptionAssert.For<ArgumentException>().Throws("../data", static value => Configurator.ResolveValidatedDataDirectory(value));
        _ = NodeExceptionAssert.For<ArgumentException>().Throws("../Squirix.settings.json", static value => Configurator.ResolveValidatedFilePath(value));
    }

    /// <summary>ApplyRuntimeDefaults canonicalizes an existing data directory.</summary>
    [Test]
    public async Task RuntimeDefaultsCanonicalizeDataDir()
    {
        var options = new SquirixServerOptions { DataDirectory = Dir };
        Configurator.ApplyRuntimeDefaults(options);
        _ = await Assert.That(options.DataDirectory).IsEqualTo(Path.GetFullPath(Dir));
    }

    /// <summary>ResolveSettingsPath validates an explicit settings path.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SettingsPathCanonicalizesExplicitInput(CancellationToken cancellationToken)
    {
        var path = Path.Join(Dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, "{}", cancellationToken);
        var resolved = Configurator.ResolveSettingsPath(path);
        _ = await Assert.That(resolved).IsEqualTo(Path.GetFullPath(path));
    }

    /// <summary>Ensures strict validation rejects invalid memory pressure thresholds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ValidateFileFlagsInvalidMemoryPressure(CancellationToken cancellationToken)
    {
        const string json =
            """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Peers":[{"NodeId":"node-a","Uri":"https://localhost:5001"}]},"MemoryPressure":{"highPressureThresholdPercent":95,"criticalPressureThresholdPercent":80}}}""";
        var path = NodePathKit.Combine(Dir, "strict.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);
        var (success, error) = await Configurator.ValidateSettingsFileAsync(path, true, cancellationToken);
        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("HighPressureThresholdPercent", StringComparison.Ordinal);
    }

    /// <summary>Ensures TryValidate surfaces multiple validation failures.</summary>
    [Test]
    public async Task ValidateReturnsErrorsWithoutThrowing()
    {
        var options = new SquirixServerOptions { NodeId = string.Empty, VirtualNodes = 0 };
        var ok = options.TryValidate(out var errors);
        _ = await Assert.That(ok).IsFalse();
        _ = await Assert.That(errors.Count >= 2).IsTrue();
    }
}
