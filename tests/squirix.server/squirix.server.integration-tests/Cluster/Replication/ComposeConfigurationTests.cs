using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Validates the RF=3 compose demos: three nodes with persistence and mTLS.</summary>
public sealed class ComposeConfigurationTests : NodeIntegrationTestBase
{
    /// <summary>Release compose matches the RF=3 layout with homogeneous images.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReleaseComposeMatchesRfThreeLayout(CancellationToken cancellationToken)
    {
        var root = RepositoryRootFinder.Find();
        var releaseCompose = await AssertComposeFileAsync(root, "docker-compose.release.yml", "SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD=", "${", false, cancellationToken);
        await AssertHomogeneousImageVersionAsync(releaseCompose);

        await AssertSettingsAsync(Path.Join(root, "docker", "node-a", "Squirix.settings.json"), "A", cancellationToken);
        await AssertSettingsAsync(Path.Join(root, "docker", "node-b", "Squirix.settings.json"), "B", cancellationToken);
        await AssertSettingsAsync(Path.Join(root, "docker", "node-c", "Squirix.settings.json"), "C", cancellationToken);
    }

    /// <summary>Three compose nodes share RF=3 peers, persistence, and homogeneous mTLS.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThreeNodesUseRfThreePersistenceAndMtls(CancellationToken cancellationToken)
    {
        var root = RepositoryRootFinder.Find();
        _ = await AssertComposeFileAsync(root, "docker-compose.yml", "SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD=", "dev-docker-mtls", true, cancellationToken);

        await AssertSettingsAsync(Path.Join(root, "docker", "node-a", "Squirix.settings.json"), "A", cancellationToken);
        await AssertSettingsAsync(Path.Join(root, "docker", "node-b", "Squirix.settings.json"), "B", cancellationToken);
        await AssertSettingsAsync(Path.Join(root, "docker", "node-c", "Squirix.settings.json"), "C", cancellationToken);
    }

    private static async Task<string> AssertComposeFileAsync(
        string root,
        string fileName,
        string pfxKey,
        string expectedValue,
        bool exactValue,
        CancellationToken cancellationToken)
    {
        var compose = await File.ReadAllTextAsync(Path.Join(root, "docker", fileName), cancellationToken);

        // Each service block wires its own port, certificate, volume, persistence, and mTLS:
        // global fragment counting would pass a miswired layout (for example port 5003 under node-a).
        await AssertServiceBlockAsync(compose, "node-a", "  node-b:", "5001:5000", "node-A.pfx", "nodea-data", (pfxKey, expectedValue, exactValue));
        await AssertServiceBlockAsync(compose, "node-b", "  node-c:", "5002:5000", "node-B.pfx", "nodeb-data", (pfxKey, expectedValue, exactValue));
        await AssertServiceBlockAsync(compose, "node-c", "\nvolumes:", "5003:5000", "node-C.pfx", "nodec-data", (pfxKey, expectedValue, exactValue));
        _ = await Assert.That(compose).Contains("nodeb-data", StringComparison.Ordinal);
        _ = await Assert.That(compose).Contains("nodec-data", StringComparison.Ordinal);
        return compose;
    }

    private static async Task AssertContainsInBlockAsync(string compose, int start, int end, string fragment, string service)
    {
        var index = compose.IndexOf(fragment, start, end - start, StringComparison.Ordinal);
        _ = await Assert.That(index >= 0).IsTrue().Because($"Service '{service}' block does not contain '{fragment}'.");
    }

    /// <summary>Asserts all three services build the same image version without pinning any version value.</summary>
    /// <param name="compose">Compose file text.</param>
    private static async Task AssertHomogeneousImageVersionAsync(string compose)
    {
        const string prefix = "SQUIRIX_VERSION:";
        var starts = new int[8];
        var lengths = new int[8];
        var count = 0;
        var index = 0;
        while ((index = compose.IndexOf(prefix, index, StringComparison.Ordinal)) >= 0 && count < starts.Length)
        {
            var valueStart = index + prefix.Length;
            var lineEnd = compose.IndexOf('\n', valueStart);
            var valueEnd = lineEnd < 0 ? compose.Length : lineEnd;
            var isEmpty = compose.AsSpan(valueStart, valueEnd - valueStart).Trim().IsEmpty;
            _ = await Assert.That(isEmpty).IsFalse().Because("Image version value must not be empty.");
            starts[count] = valueStart;
            lengths[count] = valueEnd - valueStart;
            count++;
            index = valueEnd;
        }

        _ = await Assert.That(count).IsEqualTo(3);
        for (var i = 1; i < count; i++)
            _ = await Assert.That(string.Equals(VersionAt(compose, starts[i], lengths[i]), VersionAt(compose, starts[0], lengths[0]), StringComparison.Ordinal)).IsTrue().Because("All three nodes must build the same image version.");
    }

    private static async Task AssertPfxValueAsync(string compose, int start, int end, string service, string pfxKey, string expectedValue, bool exactValue)
    {
        var keyIndex = compose.IndexOf(pfxKey, start, end - start, StringComparison.Ordinal);
        _ = await Assert.That(keyIndex >= 0).IsTrue().Because($"Service '{service}' block does not contain '{pfxKey}'.");
        var valueIndex = keyIndex + pfxKey.Length;
        var fitsInBlock = valueIndex + expectedValue.Length <= end;
        _ = await Assert.That(fitsInBlock).IsTrue().Because($"Service '{service}' has an unexpected PFX password value.");
        var matches = string.Compare(compose, valueIndex, expectedValue, 0, expectedValue.Length, StringComparison.Ordinal) == 0;
        _ = await Assert.That(matches).IsTrue().Because($"Service '{service}' has an unexpected PFX password value.");
        if (exactValue)
        {
            var valueEnd = valueIndex + expectedValue.Length;
            var terminated = valueEnd < compose.Length && (compose[valueEnd] == '\r' || compose[valueEnd] == '\n');
            _ = await Assert.That(terminated).IsTrue().Because($"Service '{service}' has an unexpected PFX password value suffix.");
        }
    }

    private static async Task AssertServiceBlockAsync(
        string compose,
        string service,
        string nextMarker,
        string port,
        string cert,
        string volume,
        (string Key, string Expected, bool Exact) pfx)
    {
        var start = compose.IndexOf("  " + service + ":", StringComparison.Ordinal);
        _ = await Assert.That(start >= 0).IsTrue().Because($"Service '{service}' was not found.");
        var end = compose.IndexOf(nextMarker, start, StringComparison.Ordinal);
        _ = await Assert.That(end > start).IsTrue().Because($"Service '{service}' block is not followed by '{nextMarker}'.");

        await AssertContainsInBlockAsync(compose, start, end, port, service);
        await AssertContainsInBlockAsync(compose, start, end, cert, service);
        await AssertContainsInBlockAsync(compose, start, end, volume + ":/data", service);
        await AssertContainsInBlockAsync(compose, start, end, "SQUIRIX_CLUSTER_MTLS_CA_PATH=/mtls/cluster-ca.crt", service);
        await AssertPfxValueAsync(compose, start, end, service, pfx.Key, pfx.Expected, pfx.Exact);
        await AssertContainsInBlockAsync(compose, start, end, "SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT=5100", service);
        await AssertContainsInBlockAsync(compose, start, end, "\"run\", \"--persist\", \"--data-dir\", \"/data\"", service);
    }

    private static async Task AssertSettingsAsync(string path, string nodeId, CancellationToken cancellationToken)
    {
        _ = await Assert.That(File.Exists(path)).IsTrue().Because($"Settings file is missing at '{path}'.");
        await using var stream = File.OpenRead(path);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var cluster = document.RootElement.GetProperty("Squirix").GetProperty("Cluster");
        _ = await Assert.That(cluster.GetProperty("NodeId").GetString()).IsEqualTo(nodeId);
        _ = await Assert.That(cluster.GetProperty("ReplicaCount").GetInt32()).IsEqualTo(3);
        _ = await Assert.That(cluster.GetProperty("PersistenceEnabled").GetBoolean()).IsTrue();
        _ = await Assert.That(cluster.GetProperty("ReplicationEnabled").GetBoolean()).IsTrue();

        var uri = cluster.GetProperty("Uri").GetString();
        _ = await Assert.That(string.IsNullOrWhiteSpace(uri)).IsFalse();
        var peers = cluster.GetProperty("Peers");
        _ = await Assert.That(peers.GetArrayLength()).IsEqualTo(3);

        var localMatch = false;
        for (var i = 0; i < peers.GetArrayLength(); i++)
        {
            var peerId = peers[i].GetProperty("NodeId").GetString();
            var peerUri = peers[i].GetProperty("Uri").GetString();
            _ = await Assert.That(string.IsNullOrWhiteSpace(peerId)).IsFalse();
            _ = await Assert.That(string.IsNullOrWhiteSpace(peerUri)).IsFalse();
            if (string.Equals(peerId, nodeId, StringComparison.Ordinal) && string.Equals(peerUri, uri, StringComparison.Ordinal))
                localMatch = true;
        }

        _ = await Assert.That(localMatch).IsTrue().Because($"Cluster.Uri must match the local peer entry in '{path}'.");
    }

    private static string VersionAt(string compose, int start, int length) => compose.AsSpan(start, length).Trim().ToString();
}
