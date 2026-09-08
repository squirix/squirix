using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>Verifies the standalone server host doctor command reports replica diagnostics.</summary>
public sealed class DoctorCommandTests : NodeIntegrationTestBase
{
    /// <summary>Verifies doctor reports inactive replication when persistence is disabled.</summary>
    [Fact]
    public async Task DoctorReportsInactiveReplication()
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-inactive");
        var settingsPath = await WriteSettingsAsync(dir.Path, 1, DefaultCancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, false, DefaultCancellationToken);

        Assert.Equal(0, exitCode);
        Assert.Contains("[Squirix.Server] Doctor", output, StringComparison.Ordinal);
        Assert.Contains("Replication: not activated (persistence disabled)", output, StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor reports a stamped fingerprint disagreeing with settings.</summary>
    [Fact]
    public async Task DoctorReportsFingerprintMismatch()
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-mismatch");
        var settingsPath = await WriteSettingsAsync(dir.Path, 2, DefaultCancellationToken);
        var dataDir = Path.Join(dir.Path, "data");
        _ = Directory.CreateDirectory(dataDir);
        var options = await Configurator.LoadFromFileAsync(settingsPath, DefaultCancellationToken);
        var expected = TopologyFingerprint.CreateFromTopology(Configurator.ToClusterConfig(options), MtlsOptionsResolver.ResolveFromEnvironment());
        var expectedHex = expected.ToString();
        var wrong = new byte[expected.Bytes.Length];
        expected.Bytes.CopyTo(wrong);
        wrong[0] ^= 0xFF;
        await new ActivatedTopologyStampStore(dataDir).PublishAsync(
            new ActivatedTopologyStamp { Generation = 5, Fingerprint = new ReadOnlyMemory<byte>(wrong), ReplicaCount = 2 },
            DefaultCancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, DefaultCancellationToken);

        Assert.Equal(0, exitCode);
        Assert.Contains("topology stamp: fingerprint MISMATCH", output, StringComparison.Ordinal);
        Assert.Contains(Convert.ToHexString(wrong), output, StringComparison.Ordinal);
        Assert.Contains(expectedHex, output, StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor reports durable group term, commit, and apply lag.</summary>
    [Fact]
    public async Task DoctorReportsGroupCommitLag()
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-lag");
        var settingsPath = await WriteSettingsAsync(dir.Path, 2, DefaultCancellationToken);
        var dataDir = Path.Join(dir.Path, "data");
        _ = Directory.CreateDirectory(dataDir);
        var options = await Configurator.LoadFromFileAsync(settingsPath, DefaultCancellationToken);
        var expected = TopologyFingerprint.CreateFromTopology(Configurator.ToClusterConfig(options), MtlsOptionsResolver.ResolveFromEnvironment());
        var expectedBytes = new byte[expected.Bytes.Length];
        expected.Bytes.CopyTo(expectedBytes);
        await new ActivatedTopologyStampStore(dataDir).PublishAsync(
            new ActivatedTopologyStamp { Generation = 5, Fingerprint = new ReadOnlyMemory<byte>(expectedBytes), ReplicaCount = 2 },
            DefaultCancellationToken);
        var meta = new GroupLogMetadata("n1", new ReadOnlyMemory<byte>(expectedBytes), 5, 9, string.Empty, 12, 10, 7);
        var buffer = new byte[GroupLogCodec.ComputeMetaEncodedLength(meta)];
        GroupLogCodec.EncodeMeta(meta, buffer);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dataDir, "n1"));
        await File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dataDir, "n1"), buffer, DefaultCancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, DefaultCancellationToken);

        Assert.Equal(0, exitCode);
        Assert.Contains("group 'n1': term 9 commit 10 applied 7 apply-lag 3", output, StringComparison.Ordinal);
        Assert.Contains("fingerprint match", output, StringComparison.Ordinal);
    }

    private static string FindHostDll()
    {
        var directory = AppContext.BaseDirectory;
        while (directory != null)
        {
            if (File.Exists(Path.Join(directory, "squirix.slnx")))
                break;

            directory = Directory.GetParent(directory)?.FullName;
        }

        if (directory == null)
            throw new InvalidOperationException("Repository root was not found.");

        var config = AppContext.BaseDirectory.Contains(
            $"{Path.DirectorySeparatorChar}Release{Path.DirectorySeparatorChar}",
            StringComparison.Ordinal) ? "Release" : "Debug";
        var hostDll = Path.Join(directory, "src", "squirix.server.host", "bin", config, "net10.0", "Squirix.Server.Host.dll");
        Assert.True(File.Exists(hostDll), $"Server host binary was not found at '{hostDll}'.");
        return hostDll;
    }

    private static async Task<(int ExitCode, string Output)> RunDoctorAsync(string settingsPath, string? dataDir, bool persist, CancellationToken cancellationToken)
    {
        var arguments = $"exec \"{FindHostDll()}\" doctor --settings \"{settingsPath}\"";
        if (dataDir != null)
            arguments += $" --data-dir \"{dataDir}\"";
        if (persist)
            arguments += " --persist";
        var started = Process.Start(new ProcessStartInfo("dotnet", arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        });
        Assert.NotNull(started);
        using var process = started;

        var outputTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var errorsTask = process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);
        var output = await outputTask;
        var errors = await errorsTask;
        return (process.ExitCode, output + errors);
    }

    private static async Task<string> WriteSettingsAsync(string dir, int replicaCount, CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var dataDir = Path.Join(dir, "data").Replace('\\', '/');
        var persistence = replicaCount > 1 ? $",\"PersistenceEnabled\":true,\"DataDirectory\":\"{dataDir}\"" : string.Empty;
        var peers = replicaCount > 1
            ? $",\"Peers\":[{{\"NodeId\":\"n1\",\"Uri\":\"{uriA.AbsoluteUri}\"}},{{\"NodeId\":\"n2\",\"Uri\":\"{uriB.AbsoluteUri}\"}}]"
            : string.Empty;
        var json = $"{{\"Squirix\":{{\"Cluster\":{{\"ClusterId\":\"doctor-c\",\"NodeId\":\"n1\",\"Uri\":\"{uriA.AbsoluteUri}\",\"ReplicaCount\":{replicaCount},\"ConfigurationGeneration\":5{persistence}{peers}}}}}}}";
        var path = Path.Join(dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);
        return path;
    }
}
