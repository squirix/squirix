using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Opt-in for the graceful transfer protocol while the architecture decision is under evaluation.</summary>
internal static class LeadershipTransfer
{
    internal static readonly bool Enabled = string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_EXPERIMENTAL_LEADERSHIP_TRANSFER"), "1", StringComparison.Ordinal);
}
