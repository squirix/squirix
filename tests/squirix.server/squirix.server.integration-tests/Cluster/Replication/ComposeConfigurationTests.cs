using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Validates the RF=3 compose demos: three nodes with persistence and mTLS.</summary>
public sealed class ComposeConfigurationTests : NodeIntegrationTestBase
{
    /// <summary>Three compose nodes share RF=3 peers, persistence, and homogeneous mTLS.</summary>
    [Fact]
    public async Task ThreeNodesUseRfThreePersistenceAndMtls()
    {
        var root = RepositoryRootFinder.Find();
        _ = await AssertComposeFileAsync(root, "docker-compose.yml", "SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD=dev-docker-mtls", DefaultCancellationToken);

        await AssertSettingsAsync(Path.Join(root, "docker", "node-a", "Squirix.settings.json"), "A", DefaultCancellationToken);
        await AssertSettingsAsync(Path.Join(root, "docker", "node-b", "Squirix.settings.json"), "B", DefaultCancellationToken);
        await AssertSettingsAsync(Path.Join(root, "docker", "node-c", "Squirix.settings.json"), "C", DefaultCancellationToken);
    }

    /// <summary>Release compose matches the RF=3 layout with homogeneous images.</summary>
    [Fact]
    public async Task ReleaseComposeMatchesRfThreeLayout()
    {
        var root = RepositoryRootFinder.Find();
        var releaseCompose = await AssertComposeFileAsync(root, "docker-compose.release.yml", "SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD=${", DefaultCancellationToken);
        AssertHomogeneousImageVersion(releaseCompose);

        await AssertSettingsAsync(Path.Join(root, "docker", "node-a", "Squirix.settings.json"), "A", DefaultCancellationToken);
        await AssertSettingsAsync(Path.Join(root, "docker", "node-b", "Squirix.settings.json"), "B", DefaultCancellationToken);
        await AssertSettingsAsync(Path.Join(root, "docker", "node-c", "Squirix.settings.json"), "C", DefaultCancellationToken);
    }

    private static async Task<string> AssertComposeFileAsync(string root, string fileName, string passwordFragment, CancellationToken cancellationToken)
    {
        var compose = await File.ReadAllTextAsync(Path.Join(root, "docker", fileName), cancellationToken);

        // Each service block wires its own port, certificate, volume, persistence, and mTLS:
        // global fragment counting would pass a miswired layout (for example port 5003 under node-a).
        AssertServiceBlock(compose, "node-a", "  node-b:", "5001:5000", "node-A.pfx", "nodea-data", passwordFragment);
        AssertServiceBlock(compose, "node-b", "  node-c:", "5002:5000", "node-B.pfx", "nodeb-data", passwordFragment);
        AssertServiceBlock(compose, "node-c", "\nvolumes:", "5003:5000", "node-C.pfx", "nodec-data", passwordFragment);

        Assert.Contains("nodea-data", compose, StringComparison.Ordinal);
        Assert.Contains("nodeb-data", compose, StringComparison.Ordinal);
        Assert.Contains("nodec-data", compose, StringComparison.Ordinal);
        return compose;
    }

    /// <summary>Asserts all three services build the same image version without pinning any version value.</summary>
    /// <param name="compose">Compose file text.</param>
    private static void AssertHomogeneousImageVersion(string compose)
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
            var trimmed = compose.AsSpan(valueStart, valueEnd - valueStart).Trim();
            Assert.False(trimmed.IsEmpty, "Image version value must not be empty.");
            starts[count] = valueStart;
            lengths[count] = valueEnd - valueStart;
            count++;
            index = valueEnd;
        }

        Assert.Equal(3, count);
        for (var i = 1; i < count; i++)
        {
            Assert.True(
                compose.AsSpan(starts[i], lengths[i]).Trim().SequenceEqual(compose.AsSpan(starts[0], lengths[0]).Trim()),
                "All three nodes must build the same image version.");
        }
    }

    private static void AssertServiceBlock(string compose, string service, string nextMarker, string port, string cert, string volume, string passwordFragment)
    {
        var start = compose.IndexOf("  " + service + ":", StringComparison.Ordinal);
        Assert.True(start >= 0, $"Service '{service}' was not found.");
        var end = compose.IndexOf(nextMarker, start, StringComparison.Ordinal);
        Assert.True(end > start, $"Service '{service}' block is not followed by '{nextMarker}'.");

        AssertContainsInBlock(compose, start, end, port, service);
        AssertContainsInBlock(compose, start, end, cert, service);
        AssertContainsInBlock(compose, start, end, volume + ":/data", service);
        AssertContainsInBlock(compose, start, end, "SQUIRIX_CLUSTER_MTLS_CA_PATH=/mtls/cluster-ca.crt", service);
        AssertContainsInBlock(compose, start, end, passwordFragment, service);
        AssertContainsInBlock(compose, start, end, "SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT=5100", service);
        AssertContainsInBlock(compose, start, end, "\"run\", \"--persist\", \"--data-dir\", \"/data\"", service);
    }

    private static void AssertContainsInBlock(string compose, int start, int end, string fragment, string service)
    {
        var index = compose.IndexOf(fragment, start, end - start, StringComparison.Ordinal);
        Assert.True(index >= 0, $"Service '{service}' block does not contain '{fragment}'.");
    }

    private static async Task AssertSettingsAsync(string path, string nodeId, CancellationToken cancellationToken)
    {
        Assert.True(File.Exists(path), $"Settings file is missing at '{path}'.");
        await using var stream = File.OpenRead(path);
        using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var cluster = document.RootElement.GetProperty("Squirix").GetProperty("Cluster");
        Assert.Equal(nodeId, cluster.GetProperty("NodeId").GetString());
        Assert.Equal(3, cluster.GetProperty("ReplicaCount").GetInt32());
        Assert.True(cluster.GetProperty("PersistenceEnabled").GetBoolean());
        Assert.True(cluster.GetProperty("ReplicationEnabled").GetBoolean());

        var uri = cluster.GetProperty("Uri").GetString();
        Assert.False(string.IsNullOrWhiteSpace(uri));
        var peers = cluster.GetProperty("Peers");
        Assert.Equal(3, peers.GetArrayLength());

        var localMatch = false;
        for (var i = 0; i < peers.GetArrayLength(); i++)
        {
            var peerId = peers[i].GetProperty("NodeId").GetString();
            var peerUri = peers[i].GetProperty("Uri").GetString();
            Assert.False(string.IsNullOrWhiteSpace(peerId));
            Assert.False(string.IsNullOrWhiteSpace(peerUri));
            if (string.Equals(peerId, nodeId, StringComparison.Ordinal) && string.Equals(peerUri, uri, StringComparison.Ordinal))
                localMatch = true;
        }

        Assert.True(localMatch, $"Cluster.Uri must match the local peer entry in '{path}'.");
    }
}
