using System;
using System.Collections.Generic;

namespace Squirix.Server;

/// <summary>
/// Configures a Squirix node hosted by an ASP.NET Core application.
/// </summary>
public sealed class SquirixServerOptions
{
    /// <summary>
    /// Gets or sets the cluster identifier.
    /// </summary>
    public string ClusterId { get; set; } = "cluster";

    /// <summary>
    /// Gets or sets a value indicating whether WAL/snapshot persistence is enabled.
    /// </summary>
    public bool PersistenceEnabled { get; set; }

    /// <summary>
    /// Gets or sets an optional persistence data directory override.
    /// </summary>
    public string? DataDirectory { get; set; }

    /// <summary>
    /// Gets or sets the local node identifier.
    /// </summary>
    public string NodeId { get; set; } = "node";

    /// <summary>
    /// Gets or sets the configured cluster peers. When empty, the local node is added automatically at runtime.
    /// </summary>
    public List<SquirixServerPeerOptions> Peers { get; set; } = [];

    /// <summary>
    /// Gets or sets the primary HTTP/2 URL used for gRPC and node traffic.
    /// </summary>
    public Uri Url { get; set; } = new("https://localhost:5001");

    /// <summary>
    /// Gets or sets the number of consistent-hash virtual nodes.
    /// </summary>
    public int VirtualNodes { get; set; } = 128;

    /// <summary>
    /// Gets or sets a value indicating whether startup waits for journal recovery before serving traffic.
    /// </summary>
    public bool WaitForRecovery { get; set; } = true;

    /// <summary>
    /// Enables WAL/snapshot persistence for this node.
    /// </summary>
    /// <param name="dataDirectory">Optional data directory override.</param>
    public void UsePersistence(string? dataDirectory = null)
    {
        PersistenceEnabled = true;
        if (!string.IsNullOrWhiteSpace(dataDirectory))
            DataDirectory = dataDirectory;
    }

    /// <summary>
    /// Validates the current configuration without throwing.
    /// </summary>
    /// <param name="errors">Validation errors when the method returns <see langword="false" />.</param>
    /// <returns><see langword="true" /> when configuration is valid.</returns>
    public bool TryValidate(out IReadOnlyList<string> errors) => ClusterTopologyValidator.TryValidate(this, out errors);

    /// <summary>
    /// Validates the current configuration and throws when a value is invalid.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when a configuration value is invalid.</exception>
    public void Validate() => SquirixServerOptionsValidator.Validate(this);
}
