using System;

namespace Squirix.Server;

/// <summary>Describes a Squirix cluster peer for a native Squirix node.</summary>
public sealed class SquirixServerPeerOptions
{
    /// <summary>Gets or sets the peer node identifier.</summary>
    public required string NodeId { get; set; }

    /// <summary>Gets or sets the peer server URI.</summary>
    public required Uri Uri { get; set; }
}
