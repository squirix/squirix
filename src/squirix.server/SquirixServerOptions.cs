using System;
using System.Collections.Generic;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server;

/// <summary>Configures a Squirix node hosted by an ASP.NET Core application.</summary>
public sealed class SquirixServerOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether the replica groups of this node elect a new leader when the current one goes silent.
    /// Default is <see langword="false" />: the owner of each key leads its replica group in term one for the lifetime of the topology.
    /// </summary>
    /// <remarks>
    /// Requires <see cref="ReplicaCount" /> of at least 3 and <see cref="QuorumReadsEnabled" />; <see cref="Validate()" /> refuses any other
    /// combination. Every node of a cluster must use the same value: the value is an input of the topology fingerprint, so a node with a
    /// different value refuses replication with its peers, and a data directory activated with one value refuses a start with the other.
    /// The settings key is <c language="csharp">Squirix:Cluster:AutomaticFailoverEnabled</c>.
    /// </remarks>
    public bool AutomaticFailoverEnabled { get; set; }

    /// <summary>Gets or sets the node-level admission control (backpressure) options.</summary>
    /// <remarks>Must not be <see langword="null" />. Changes apply on the next host start.</remarks>
    public SquirixServerBackpressureOptions Backpressure { get; set; } = new();

    /// <summary>Gets or sets the cluster identifier.</summary>
    public string ClusterId { get; set; } = "cluster";

    /// <summary>
    /// Gets or sets the configuration generation of the cluster topology.
    /// Default is <c language="csharp">1</c>; must be greater than zero.
    /// </summary>
    /// <remarks>
    /// The generation is an input of the topology fingerprint. The first RF&gt;1 start with persistence records it in the
    /// activated topology stamp of the data directory, and every later RF&gt;1 start on that directory refuses a different
    /// value: changing the activated topology of an existing data directory is not supported.
    /// </remarks>
    public ulong ConfigurationGeneration { get; set; } = 1;

    /// <summary>Gets or sets an optional persistence data directory override.</summary>
    public string? DataDirectory { get; set; }

    /// <summary>
    /// Gets or sets the longest time a client call forwarded to a cluster peer may wait for its connection to the peer to be established,
    /// before any TLS handshake. Default is 300 milliseconds ("00:00:00.300" in settings); must be positive and below the three-second
    /// per-attempt timeout of a forwarded call.
    /// </summary>
    /// <remarks>
    /// It applies to client forwards only; replication and election traffic keeps its own longer bounds. A host that is down or drops
    /// connection attempts never completes the connection, so the forward fails as unreachable, having sent nothing, instead of ending in an
    /// ambiguous timeout. A live peer completes it at once; the TLS handshake that follows keeps a longer bound, so a loaded peer is not cut
    /// off. Changes apply on the next host start.
    /// </remarks>
    public TimeSpan InterNodeConnectTimeout { get; set; } = TopologyOptions.DefaultInterNodeConnectTimeout;

    /// <summary>Gets or sets the node journal options, including journal group commit.</summary>
    /// <remarks>Must not be <see langword="null" />. Requires persistence for group commit. Changes apply on the next host start.</remarks>
    public SquirixServerJournalOptions Journal { get; set; } = new();

    /// <summary>Gets or sets the local node identifier.</summary>
    public string NodeId { get; set; } = "node";

    /// <summary>Gets or sets the configured cluster peers. When empty, the local node is added automatically at runtime.</summary>
    public IReadOnlyList<SquirixServerPeerOptions> Peers { get; set; } = Array.Empty<SquirixServerPeerOptions>();

    /// <summary>Gets or sets a value indicating whether journal/snapshot persistence is enabled.</summary>
    public bool PersistenceEnabled { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a read of a replicated key is confirmed by a majority of its replica group before it is
    /// served, so that it observes every write committed before it. Default is <see langword="false" />: a read is served from the local
    /// state of the group owner.
    /// </summary>
    /// <remarks>
    /// Requires <see cref="AutomaticFailoverEnabled" />, which requires it in turn: quorum reads are served by elected leaders only. Part of
    /// the topology fingerprint like <see cref="AutomaticFailoverEnabled" />. The settings key is
    /// <c language="csharp">Squirix:Cluster:QuorumReadsEnabled</c>.
    /// </remarks>
    public bool QuorumReadsEnabled { get; set; }

    /// <summary>
    /// Gets or sets the replica factor including the original owner.
    /// Default is <c language="csharp">1</c>; must be between 1 and 5 and cannot exceed the number of configured peers.
    /// </summary>
    /// <remarks>
    /// A value greater than one activates replication. It requires persistence (see <see cref="UsePersistence" />), which
    /// <see cref="Validate()" /> checks, and at host startup also cluster mTLS material and the <see cref="ReplicationEnabled" />
    /// opt-in; startup is refused when any of them is missing. The first RF&gt;1 start records the replica count in the
    /// activated topology stamp of the data directory, and every later RF&gt;1 start on that directory refuses a different value.
    /// </remarks>
    public int ReplicaCount { get; set; } = 1;

    /// <summary>
    /// Gets or sets a value indicating whether the operator explicitly opts into RF&gt;1 replication.
    /// Default is <see langword="false" />: RF&gt;1 startup without the opt-in is refused.
    /// </summary>
    public bool ReplicationEnabled { get; set; }

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

    /// <summary>Maps the options to the cluster topology the node host runs with, without validating them.</summary>
    /// <param name="options">Server options.</param>
    /// <returns>The cluster topology.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <see cref="Peers" /> or <see cref="Uri" /> is <see langword="null" />.</exception>
    internal static TopologyOptions BuildTopology(SquirixServerOptions options)
    {
        var peerOptions = options.Peers;
        var uri = options.Uri;
        if (peerOptions == null)
            throw new ArgumentNullException(nameof(options), "Peers cannot be null.");

        if (uri == null)
            throw new ArgumentNullException(nameof(options), "Uri cannot be null.");

        var peers = new ServerPeer[peerOptions.Count == 0 ? 1 : peerOptions.Count];
        if (peerOptions.Count == 0)
        {
            peers[0] = new ServerPeer { NodeId = options.NodeId, Uri = uri };
        }
        else
        {
            for (var i = 0; i < peerOptions.Count; i++)
                peers[i] = new ServerPeer { NodeId = peerOptions[i].NodeId, Uri = peerOptions[i].Uri };
        }

        return new TopologyOptions(peers)
        {
            ClusterId = options.ClusterId,
            NodeId = options.NodeId,
            Uri = uri,
            VirtualNodes = options.VirtualNodes,
            ReplicaCount = options.ReplicaCount,
            ReplicationEnabled = options.ReplicationEnabled,
            ConfigurationGeneration = options.ConfigurationGeneration,
            AutomaticFailoverEnabled = options.AutomaticFailoverEnabled,
            QuorumReadsEnabled = options.QuorumReadsEnabled,
            InterNodeConnectTimeout = options.InterNodeConnectTimeout,
        };
    }

    private static bool TryValidateOptions(SquirixServerOptions options, out IReadOnlyList<string> errors)
    {
        ArgumentNullException.ThrowIfNull(options);

        var topology = BuildTopology(options);

        if (!TopologyValidator.TryValidate(topology, options.PersistenceEnabled, options.DataDirectory, out errors))
            return false;

        var sectionFailure = ValidateSections(options);
        if (sectionFailure != null)
        {
            errors = [sectionFailure];
            return false;
        }

        // Public options path does not carry mTLS material and does not enforce the replication opt-in:
        // the opt-in is a hosting activation concern evaluated by ReplicationActivationGuard at startup.
        var activationFailures = new List<string>();
        ReplicationActivationGuard.CollectFailures(activationFailures, options.ReplicaCount, options.PersistenceEnabled, null, true);
        if (activationFailures.Count == 0)
            return true;

        errors = activationFailures;
        return false;
    }

    private static string? ValidateSections(SquirixServerOptions options)
    {
        if (options.Backpressure == null)
            return "Backpressure cannot be null.";

        try
        {
            options.Backpressure.ToAdmissionOptions().Validate();
        }
        catch (InvalidOperationException ex)
        {
            return ex.Message;
        }

        if (options.Journal == null)
            return "Journal cannot be null.";

        try
        {
            options.Journal.ToPersistenceOptions().Validate();
        }
        catch (InvalidOperationException ex)
        {
            return ex.Message;
        }

        return options.Journal.GroupCommitMaxWait > TimeSpan.Zero && !options.PersistenceEnabled
            ? "Journal GroupCommitMaxWait greater than zero requires persistence. Set PersistenceEnabled."
            : ForwardingCallPolicyDefaults.ValidateConnectTimeout(options.InterNodeConnectTimeout);
    }

    private static void Validate(SquirixServerOptions options)
    {
        if (!options.TryValidate(out var errors))
            throw new ArgumentException(errors[0], nameof(options));
    }
}
