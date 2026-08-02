namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Canonical namespace roots for types compiled into <see cref="Server" />.
/// </summary>
internal static class ServerArchitectureNamespaces
{
    internal const string Cluster = Root + ".Cluster";

    internal const string Node = Root + ".Node";

    internal const string PackageId = "squirix.server";

    internal const string Root = "Squirix.Server";

    internal const string Storage = Root + ".Storage";
}
