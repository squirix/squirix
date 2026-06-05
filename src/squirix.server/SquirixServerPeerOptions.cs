using System;

namespace Squirix.Server;

/// <summary>
/// Describes a Squirix cluster peer for ASP.NET Core custom hosting.
/// </summary>
public sealed class SquirixServerPeerOptions
{
    /// <summary>
    /// Gets or sets the peer node identifier.
    /// </summary>
    public required string NodeId { get; set; }

    /// <summary>
    /// Gets or sets the peer server URL.
    /// </summary>
    public required Uri Url { get; set; }
}
