using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Verifies the standalone server host doctor command reports replica diagnostics.</summary>
public sealed class DoctorCommandTests : NodeIntegrationTestBase
{
    private const string MismatchError =
        "[Squirix.Server] Error: Durable replica state in the data directory does not match the configured topology; " +
        "see the MISMATCH, UNREADABLE, or MISSING lines above.";

    /// <summary>Verifies doctor honors the replication opt-in passed on the command line.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorHonorsReplicationOptInFlag(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-optin");
        var settingsPath = await WriteSettingsAsync(dir, 2, cancellationToken, false);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, cancellationToken, true);
        _ = await Assert.That(exitCode).IsEqualTo(0);
        _ = await Assert.That(output).Contains("[Squirix.Server] Doctor", StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor without a data directory reads the stamp in the default data directory hosting uses and exits with code 1.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorReportsDefaultDirMismatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-default-mismatch");
        var settingsPath = await WriteSettingsAsync(dir, 2, cancellationToken, withDataDirectory: false);
        var testRoot = Path.Join(dir, "root");
        var dataDir = Path.GetFullPath(Path.Join(testRoot, "doctor-c", "n1"));
        _ = Directory.CreateDirectory(dataDir);
        var options = await Configurator.LoadAsync(settingsPath, cancellationToken);
        var expected = TopologyFingerprint.CreateFromTopology(Configurator.ToClusterConfig(options), MtlsOptionsResolver.ResolveFromEnvironment());
        var wrong = new byte[expected.Bytes.Length];
        expected.Bytes.CopyTo(wrong);
        wrong[0] ^= 0xFF;
        await new ActivatedTopologyStampStore(dataDir).PublishAsync(
            new ActivatedTopologyStamp { Generation = 5, Fingerprint = new ReadOnlyMemory<byte>(wrong), ReplicaCount = 2 },
            cancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, cancellationToken, testRoot: testRoot);

        _ = await Assert.That(exitCode).IsEqualTo(1);
        _ = await Assert.That(output).Contains($"  Persistence: enabled (data dir: {dataDir}, default)", StringComparison.Ordinal);
        _ = await Assert.That(output).Contains("topology stamp: fingerprint MISMATCH", StringComparison.Ordinal);
        _ = await Assert.That(output).Contains(MismatchError, StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor reports an empty RF&gt;1 data directory as not activated and exits with code 0.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorReportsEmptyDataDirectory(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-empty");
        var settingsPath = await WriteSettingsAsync(dir, 2, cancellationToken);
        _ = Directory.CreateDirectory(Path.Join(dir, "data"));

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, cancellationToken);

        _ = await Assert.That(exitCode).IsEqualTo(0);
        _ = await Assert.That(output).Contains("  topology stamp: not activated", StringComparison.Ordinal);
        _ = await Assert.That(output).DoesNotContain("Error:", StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor without a data directory inspects an empty default data directory and exits with code 0.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorReportsEmptyDefaultDir(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-default-empty");
        var settingsPath = await WriteSettingsAsync(dir, 2, cancellationToken, withDataDirectory: false);
        var testRoot = Path.Join(dir, "root");
        var dataDir = Path.GetFullPath(Path.Join(testRoot, "doctor-c", "n1"));

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, cancellationToken, testRoot: testRoot);

        _ = await Assert.That(exitCode).IsEqualTo(0);
        _ = await Assert.That(output).Contains($"  Persistence: enabled (data dir: {dataDir}, default)", StringComparison.Ordinal);
        _ = await Assert.That(output).Contains("  Data directory access: writable", StringComparison.Ordinal);
        _ = await Assert.That(output).Contains("  topology stamp: not activated", StringComparison.Ordinal);
        _ = await Assert.That(output).DoesNotContain("Error:", StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor reports a stamped fingerprint disagreeing with settings and exits with code 1.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorReportsFingerprintMismatch(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-mismatch");
        var settingsPath = await WriteSettingsAsync(dir, 2, cancellationToken);
        var dataDir = Path.Join(dir, "data");
        _ = Directory.CreateDirectory(dataDir);
        var options = await Configurator.LoadAsync(settingsPath, cancellationToken);
        var expected = TopologyFingerprint.CreateFromTopology(Configurator.ToClusterConfig(options), MtlsOptionsResolver.ResolveFromEnvironment());
        var expectedHex = expected.ToString();
        var wrong = new byte[expected.Bytes.Length];
        expected.Bytes.CopyTo(wrong);
        wrong[0] ^= 0xFF;
        await new ActivatedTopologyStampStore(dataDir).PublishAsync(
            new ActivatedTopologyStamp { Generation = 5, Fingerprint = new ReadOnlyMemory<byte>(wrong), ReplicaCount = 2 },
            cancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, cancellationToken);

        _ = await Assert.That(exitCode).IsEqualTo(1);
        _ = await Assert.That(output).Contains("topology stamp: fingerprint MISMATCH", StringComparison.Ordinal);
        _ = await Assert.That(output).Contains(Convert.ToHexString(wrong), StringComparison.Ordinal);
        _ = await Assert.That(output).Contains(expectedHex, StringComparison.Ordinal);
        _ = await Assert.That(output).Contains(MismatchError, StringComparison.Ordinal);
        _ = await Assert.That(output.IndexOf("  Configuration: valid", StringComparison.Ordinal))
                        .IsBetween(0, output.IndexOf("topology stamp:", StringComparison.Ordinal));
    }

    /// <summary>Verifies doctor reports durable group term, commit, and apply lag.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorReportsGroupCommitLag(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-lag");
        var settingsPath = await WriteSettingsAsync(dir, 2, cancellationToken);
        var dataDir = Path.Join(dir, "data");
        _ = Directory.CreateDirectory(dataDir);
        var options = await Configurator.LoadAsync(settingsPath, cancellationToken);
        var expected = TopologyFingerprint.CreateFromTopology(Configurator.ToClusterConfig(options), MtlsOptionsResolver.ResolveFromEnvironment());
        var expectedBytes = new byte[expected.Bytes.Length];
        expected.Bytes.CopyTo(expectedBytes);
        await new ActivatedTopologyStampStore(dataDir).PublishAsync(
            new ActivatedTopologyStamp { Generation = 5, Fingerprint = new ReadOnlyMemory<byte>(expectedBytes), ReplicaCount = 2 },
            cancellationToken);
        var meta = new GroupLogMetadata("n1", new ReadOnlyMemory<byte>(expectedBytes), 5, 9, string.Empty, 12, 10, 7);
        var buffer = new byte[GroupLogCodec.ComputeMetaEncodedLength(in meta)];
        GroupLogCodec.EncodeMeta(in meta, buffer);
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dataDir, "n1"));
        await File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dataDir, "n1"), buffer, cancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, cancellationToken);

        _ = await Assert.That(exitCode).IsEqualTo(0);
        _ = await Assert.That(output).Contains("group 'n1': term 9 commit 10 applied 7 apply-lag 3", StringComparison.Ordinal);
        _ = await Assert.That(output).Contains("topology stamp: fingerprint match", StringComparison.Ordinal);
        _ = await Assert.That(output).DoesNotContain("Error:", StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor reports inactive replication when persistence is disabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorReportsInactiveReplication(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-inactive");
        var settingsPath = await WriteSettingsAsync(dir, 1, cancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, false, cancellationToken);

        _ = await Assert.That(exitCode).IsEqualTo(0);
        _ = await Assert.That(output).Contains("[Squirix.Server] Doctor", StringComparison.Ordinal);
        _ = await Assert.That(output).Contains("Replication: not activated (persistence disabled)", StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor reports unstamped RF=1 journal state under an RF&gt;1 configuration and exits with code 1.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorReportsRfOneJournalState(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-rf1");
        var settingsPath = await WriteSettingsAsync(dir, 2, cancellationToken);
        var dataDir = Path.Join(dir, "data");
        _ = Directory.CreateDirectory(dataDir);
        await File.WriteAllBytesAsync(Path.Join(dataDir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}"), [1], cancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, cancellationToken);

        _ = await Assert.That(exitCode).IsEqualTo(1);
        _ = await Assert.That(output).Contains(
            "  topology stamp: MISSING while the data directory holds durable cache journal state " +
            "(last used by an RF=1 node; moving RF=1 data to RF>1 is not supported, so RF>1 startup is refused)",
            StringComparison.Ordinal);
        _ = await Assert.That(output).Contains(MismatchError, StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor reports a stamped data directory under an RF=1 configuration as the startup refusal and exits with code 1.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorReportsRfOneOnStampedDir(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-rf1-stamped");
        var settingsPath = await WriteSettingsAsync(dir, 1, cancellationToken);
        var dataDir = Path.Join(dir, "data");
        _ = Directory.CreateDirectory(dataDir);
        await new ActivatedTopologyStampStore(dataDir).PublishAsync(
            new ActivatedTopologyStamp { Generation = 5, Fingerprint = new ReadOnlyMemory<byte>(new byte[32]), ReplicaCount = 2 },
            cancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, dataDir, true, cancellationToken);

        _ = await Assert.That(exitCode).IsEqualTo(1);
        _ = await Assert.That(output).Contains(
            "  topology stamp: MISMATCH (activated for replica count 2, generation 5; starting it as RF=1 is not supported, so RF=1 startup is refused)",
            StringComparison.Ordinal);
        _ = await Assert.That(output).Contains(MismatchError, StringComparison.Ordinal);
    }

    /// <summary>Verifies doctor expects the fingerprint of the configured failover switches and reports a stamp of the other mode as a mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoctorSeesFailoverSwitches(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-doctor-cmd-failover");
        var settingsPath = await WriteSettingsAsync(dir, 3, cancellationToken, failover: true);
        var dataDir = Path.Join(dir, "data");
        _ = Directory.CreateDirectory(dataDir);
        var options = await Configurator.LoadAsync(settingsPath, cancellationToken);
        var mtls = MtlsOptionsResolver.ResolveFromEnvironment();
        var expected = TopologyFingerprint.CreateFromTopology(Configurator.ToClusterConfig(options), mtls);
        options.AutomaticFailoverEnabled = false;
        options.QuorumReadsEnabled = false;
        var stamped = TopologyFingerprint.CreateFromTopology(Configurator.ToClusterConfig(options), mtls);
        var stampedBytes = new byte[stamped.Bytes.Length];
        stamped.Bytes.CopyTo(stampedBytes);
        await new ActivatedTopologyStampStore(dataDir).PublishAsync(
            new ActivatedTopologyStamp { Generation = 5, Fingerprint = new ReadOnlyMemory<byte>(stampedBytes), ReplicaCount = 3 },
            cancellationToken);

        var (exitCode, output) = await RunDoctorAsync(settingsPath, null, true, cancellationToken);

        _ = await Assert.That(exitCode).IsEqualTo(1);
        _ = await Assert.That(output).Contains("topology stamp: fingerprint MISMATCH", StringComparison.Ordinal);
        _ = await Assert.That(output).Contains(expected.ToString(), StringComparison.Ordinal);
        _ = await Assert.That(output).Contains(stamped.ToString(), StringComparison.Ordinal);
    }

    /// <summary>Verifies the host help lists the replication opt-in switch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HelpListsReplicationOptInSwitch(CancellationToken cancellationToken)
    {
        var (exitCode, output) = await RunHostAsync($"exec \"{await FindHostDllAsync()}\" help", cancellationToken);
        _ = await Assert.That(exitCode).IsEqualTo(0);
        _ = await Assert.That(output).Contains("--enable-replication", StringComparison.Ordinal);
    }

    /// <summary>Verifies run refuses RF&gt;1 without the opt-in.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunRefusesWithoutOptIn(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-run-nooptin");
        var settingsPath = await WriteSettingsAsync(dir, 2, cancellationToken, false);
        _ = Directory.CreateDirectory(Path.Join(dir, "data"));

        var (exitCode, output) = await RunHostAsync($"exec \"{await FindHostDllAsync()}\" run --settings \"{settingsPath}\"", cancellationToken);
        _ = await Assert.That(exitCode).IsNotEqualTo(0);
        _ = await Assert.That(output).Contains("replication opt-in", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<string> FindHostDllAsync()
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

        var config = AppContext.BaseDirectory.Contains($"{Path.DirectorySeparatorChar}Release{Path.DirectorySeparatorChar}", StringComparison.Ordinal) ? "Release" : "Debug";
        var hostDll = Path.Join(directory, "src", "squirix.server.host", "bin", config, "net10.0", "Squirix.Server.Host.dll");
        _ = await Assert.That(File.Exists(hostDll)).IsTrue().Because($"Server host binary was not found at '{hostDll}'.");
        return hostDll;
    }

    private static async Task<(int ExitCode, string Output)> RunDoctorAsync(
        string settingsPath,
        string? dataDir,
        bool persist,
        CancellationToken cancellationToken,
        bool enableReplication = false,
        string? testRoot = null)
    {
        var hostDll = await FindHostDllAsync();
        var arguments = $"exec \"{hostDll}\" doctor --settings \"{settingsPath}\"";
        if (dataDir != null)
            arguments += $" --data-dir \"{dataDir}\"";
        if (persist)
            arguments += " --persist";
        if (enableReplication)
            arguments += " --enable-replication";
        return await RunHostAsync(arguments, cancellationToken, testRoot);
    }

    private static string? ResolveDotnetPath()
    {
        var dotnetRoot = Environment.GetEnvironmentVariable("DOTNET_ROOT");
        if (!string.IsNullOrWhiteSpace(dotnetRoot))
        {
            var dotnetRootCandidate = Path.Join(dotnetRoot, OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet");
            if (File.Exists(dotnetRootCandidate))
                return Path.GetFullPath(dotnetRootCandidate);
        }

        var processPath = Environment.ProcessPath;
        if (!string.IsNullOrWhiteSpace(processPath))
        {
            var processFileName = Path.GetFileName(processPath);
            if (string.Equals(processFileName, "dotnet", StringComparison.OrdinalIgnoreCase)
                || string.Equals(processFileName, "dotnet.exe", StringComparison.OrdinalIgnoreCase))
                return Path.GetFullPath(processPath);
        }

        var pathValue = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrWhiteSpace(pathValue))
            return null;

        var executableName = OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";
        foreach (var segment in pathValue.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            var pathCandidate = Path.Join(segment, executableName);
            if (File.Exists(pathCandidate))
                return Path.GetFullPath(pathCandidate);
        }

        return null;
    }

    private static async Task<(int ExitCode, string Output)> RunHostAsync(string arguments, CancellationToken cancellationToken, string? testRoot = null)
    {
        var dotnetPath = ResolveDotnetPath() ?? "dotnet";
        var info = new ProcessStartInfo(dotnetPath, arguments)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        // The child inherits this process's SQUIRIX_TEST_ROOT; a test that inspects the default data directory pins its own.
        if (testRoot != null)
            info.Environment["SQUIRIX_TEST_ROOT"] = testRoot;

        var started = Process.Start(info);
        using var process = await Assert.That(started).IsNotNull();
        var outputTask = process.StandardOutput.ReadToEndAsync(cancellationToken);
        var errorsTask = process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);
        var output = await outputTask;
        var errors = await errorsTask;
        return (process.ExitCode, output + errors);
    }

    private static async Task<string> WriteSettingsAsync(
        string dir,
        int replicaCount,
        CancellationToken cancellationToken,
        bool replicationEnabled = true,
        bool withDataDirectory = true,
        bool failover = false)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var dataDir = Path.Join(dir, "data").Replace('\\', '/');
        var dataDirectory = withDataDirectory ? $",\"DataDirectory\":\"{dataDir}\"" : string.Empty;
        var persistence = replicaCount > 1 ? $",\"PersistenceEnabled\":true{dataDirectory}" : string.Empty;
        if (replicationEnabled && replicaCount > 1)
            persistence += ",\"ReplicationEnabled\":true";
        var third = replicaCount > 2 ? $",{{\"NodeId\":\"n3\",\"Uri\":\"{GetNextHttpUri().AbsoluteUri}\"}}" : string.Empty;
        var peers = replicaCount > 1 ? $",\"Peers\":[{{\"NodeId\":\"n1\",\"Uri\":\"{uriA.AbsoluteUri}\"}},{{\"NodeId\":\"n2\",\"Uri\":\"{uriB.AbsoluteUri}\"}}{third}]" : string.Empty;
        if (failover)
            peers += ",\"AutomaticFailoverEnabled\":true,\"QuorumReadsEnabled\":true";
        var json =
            $"{{\"Squirix\":{{\"Cluster\":{{\"ClusterId\":\"doctor-c\",\"NodeId\":\"n1\",\"Uri\":\"{uriA.AbsoluteUri}\",\"ReplicaCount\":{replicaCount},\"ConfigurationGeneration\":5{persistence}{peers}}}}}}}";
        var path = Path.Join(dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);
        return path;
    }
}
