using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

/// <summary>The outcome of a successful peer certificate chain build: the NodeId of the certificate and the window in which every certificate of its chain is valid.</summary>
/// <param name="NodeId">The NodeId of the verified certificate.</param>
/// <param name="NotBefore">The latest start of validity among the certificates of the chain.</param>
/// <param name="NotAfter">The earliest end of validity among the certificates of the chain.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct PeerChainVerdict(string NodeId, DateTimeOffset NotBefore, DateTimeOffset NotAfter)
{
    /// <summary>Returns whether every certificate of the chain is valid at the given time.</summary>
    /// <param name="time">The time to check.</param>
    /// <returns><see langword="true" /> when <paramref name="time" /> lies inside the validity window of the chain.</returns>
    internal bool IsValidAt(DateTimeOffset time) => time >= NotBefore && time <= NotAfter;
}
