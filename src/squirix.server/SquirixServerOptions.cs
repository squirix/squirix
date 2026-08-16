using System;
using System.Collections.Generic;
using Squirix.Server.Cluster;

namespace Squirix.Server;

/// <summary>Configures a Squirix node hosted by an ASP.NET Core application.</summary>
public sealed class SquirixServerOptions
{
    /// <summary>Gets or sets the cluster identifier.</summary>
    public string ClusterId { get; set; } = "cluster";

    /// <summary>
    /// Gets or sets the stopped-topology configuration generation.
    /// Must be greater than zero. Changing an activated RF&gt;1 topology is unsupported.
    /// </summary>
    public ulong ConfigurationGeneration { get; set; } = 1;

    /// <summary>Gets or sets an optional persistence data directory override.</summary>
    public string? DataDirectory { get; set; }

    /// <summary>Gets or sets the local node identifier.</summary>
    public string NodeId { get; set; } = "node";

    /// <summary>Gets or sets the configured cluster peers. When empty, the local node is added automatically at runtime.</summary>
    public IReadOnlyList<SquirixServerPeerOptions> Peers { get; set; } = Array.Empty<SquirixServerPeerOptions>();

    /// <summary>Gets or sets a value indicating whether journal/snapshot persistence is enabled.</summary>
    public bool PersistenceEnabled { get; set; }

    /// <summary>
    /// Gets or sets the replica factor including the original owner.
    /// Default is <c>1</c>. Values greater than one are planning-only until replication activation.
    /// </summary>
    public int ReplicaCount { get; set; } = 1;

    /// <summary>Gets or sets the primary HTTPS URI used for gRPC and node traffic.</summary>
    public Uri Uri { get; set; } = new("https://localhost:5001");

    /// <summary>Gets or sets the number of consistent-hash virtual nodes.</summary>
    public int VirtualNodes { get; set; } = 128;

    /// <summary>Gets or sets a value indicating whether startup waits for journal recovery before serving traffic.</summary>
    public bool WaitForRecovery { get; set; } = true;

    /// <summary>Validates the current configuration without throwing.</summary>
    /// <param name="errors">Validation errors when the method returns <see langword="false" />.</param>
    /// <returns><see langword="true" /> when configuration is valid.</returns>
    public bool TryValidate(out IReadOnlyList<string> errors) => TryValidateOptions(this, out errors);

    /// <summary>Enables journal/snapshot persistence for this node.</summary>
    /// <param name="dataDirectory">Optional data directory override.</param>
    public void UsePersistence(string? dataDirectory = null)
    {
        PersistenceEnabled = true;
        if (!string.IsNullOrWhiteSpace(dataDirectory))
            DataDirectory = dataDirectory;
    }

    /// <summary>Validates the current configuration and throws when a value is invalid.</summary>
    /// <exception cref="ArgumentException">Thrown when a configuration value is invalid.</exception>
    public void Validate() => Validate(this);

    private static bool TryValidateOptions(SquirixServerOptions options, out IReadOnlyList<string> errors)
    {
        ArgumentNullException.ThrowIfNull(options);

        var peerOptions = options.Peers;
        var uri = options.Uri;
        if (peerOptions is null)
            throw new ArgumentNullException(nameof(options), "Peers cannot be null.");

        if (uri is null)
            throw new ArgumentNullException(nameof(options), "Uri cannot be null.");

        var peers = new ServerPeer[peerOptions.Count is 0 ? 1 : peerOptions.Count];
        if (peerOptions.Count is 0)
        {
            peers[0] = new ServerPeer { NodeId = options.NodeId, Uri = uri };
        }
        else
        {
            for (var i = 0; i < peerOptions.Count; i++)
                peers[i] = new ServerPeer { NodeId = peerOptions[i].NodeId, Uri = peerOptions[i].Uri };
        }

        var topology = new TopologyOptions(peers)
        {
            ClusterId = options.ClusterId,
            NodeId = options.NodeId,
            Uri = uri,
            VirtualNodes = options.VirtualNodes,
            ReplicaCount = options.ReplicaCount,
            ConfigurationGeneration = options.ConfigurationGeneration,
        };

        if (!TopologyValidator.TryValidate(topology, options.PersistenceEnabled, options.DataDirectory, out errors))
            return false;

        // Public options path does not carry mTLS material; evaluate persistence then refuse RF>1 activation.
        var activationFailures = new List<string>();
        ReplicationActivationGuard.CollectFailures(activationFailures, options.ReplicaCount, options.PersistenceEnabled, null);
        if (activationFailures.Count is 0)
            return true;

        errors = activationFailures;
        return false;
    }

    private static void Validate(SquirixServerOptions options)
    {
        if (!options.TryValidate(out var errors))
            throw new ArgumentException(errors[0], nameof(options));
    }
}
