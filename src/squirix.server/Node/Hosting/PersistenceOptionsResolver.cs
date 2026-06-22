using System;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class PersistenceOptionsResolver
{
    public static PersistenceOptions Resolve(ClusterConfig cluster, PersistenceOptions source)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(source);

        var dataDir = string.IsNullOrWhiteSpace(source.DataDir) ? GetDefaultDataDir(cluster.ClusterId, cluster.NodeId) : source.DataDir;
        return source with { DataDir = dataDir };
    }

    private static string GetDefaultDataDir(string clusterId, string nodeId)
    {
        var testRoot = EnvVariables.ReadString("SQUIRIX_TEST_ROOT");
        if (!string.IsNullOrWhiteSpace(testRoot))
            return PathEx.Combine(testRoot, clusterId, nodeId);

        var dir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(dir) && !OperatingSystem.IsWindows())
            dir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create);

        return string.IsNullOrWhiteSpace(dir) ? throw new InvalidOperationException(
                "Cannot determine default data directory: LocalApplicationData is not available. Set PersistenceOptions.DataDir explicitly or define the HOME / XDG_DATA_HOME environment variable.")
            : PathEx.Combine(dir, "squirix", clusterId, nodeId);
    }
}
