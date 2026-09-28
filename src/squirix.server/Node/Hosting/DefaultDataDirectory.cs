using System;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>Resolves the node data directory used when persistence is enabled without an explicit directory.</summary>
/// <remarks>Shared by hosting composition and the offline doctor so both inspect the same directory.</remarks>
internal static class DefaultDataDirectory
{
    /// <summary>Resolves <c language="csharp">SQUIRIX_TEST_ROOT/&lt;cluster&gt;/&lt;node&gt;</c> when that variable is set; otherwise <c language="csharp">LocalApplicationData/squirix/&lt;cluster&gt;/&lt;node&gt;</c>.</summary>
    /// <param name="clusterId">The configured cluster identifier.</param>
    /// <param name="nodeId">The configured node identifier.</param>
    /// <returns>The absolute default data directory path.</returns>
    /// <exception cref="InvalidOperationException">Thrown when LocalApplicationData is not available and no test root is set.</exception>
    internal static string Resolve(string clusterId, string nodeId)
    {
        var testRoot = EnvVariables.ReadString("SQUIRIX_TEST_ROOT");
        if (!string.IsNullOrWhiteSpace(testRoot))
            return PathEx.Combine(testRoot, clusterId, nodeId);

        var dir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(dir) && !OperatingSystem.IsWindows())
            dir = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData, Environment.SpecialFolderOption.Create);

        return string.IsNullOrWhiteSpace(dir)
            ? throw new InvalidOperationException(
                "Cannot determine default data directory: LocalApplicationData is not available. " +
                "Set PersistenceOptions.DataDir explicitly or define the HOME / XDG_DATA_HOME environment variable.")
            : PathEx.Combine(dir, "squirix", clusterId, nodeId);
    }
}
