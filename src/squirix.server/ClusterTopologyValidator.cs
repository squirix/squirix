using System;
using System.Collections.Generic;
using Squirix.Server.Cluster.Membership;

namespace Squirix.Server;

internal static class ClusterTopologyValidator
{
    private const int MaxDataDirectoryLength = 1024;
    private const int MaxIdentifierLength = 128;
    private const int MaxPeers = 1024;
    private const int MaxUrlLength = 2048;
    private const int MaxVirtualNodes = 16384;

    internal static bool TryValidate(ClusterConfig options, out IReadOnlyList<string> errors)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();
        var args = new TopologyValidationArgs
        {
            ClusterId = options.ClusterId,
            NodeId = options.NodeId,
            NodeUri = options.Uri,
            VirtualNodes = options.VirtualNodes,
            PersistenceEnabled = true,
            DataDirectory = null,
        };
        ValidateTopology(failures, args, static peer => (peer.NodeId, peer.Uri), options.Peers);

        if (failures.Count is 0)
        {
            errors = [];
            return true;
        }

        errors = failures;
        return false;
    }

    internal static void Validate(SquirixServerOptions options)
    {
        if (!TryValidate(options, out var errors))
            throw new ArgumentException(errors[0], nameof(options));
    }

    internal static bool TryValidate(SquirixServerOptions options, out IReadOnlyList<string> errors)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();
        var args = new TopologyValidationArgs
        {
            ClusterId = options.ClusterId,
            NodeId = options.NodeId,
            NodeUri = options.Uri,
            VirtualNodes = options.VirtualNodes,
            PersistenceEnabled = options.PersistenceEnabled,
            DataDirectory = options.DataDirectory,
        };
        ValidateTopology(failures, args, static peer => (peer.NodeId, peer.Uri), options.Peers);

        if (failures.Count is 0)
        {
            errors = [];
            return true;
        }

        errors = failures;
        return false;
    }

    private static void ValidateIdentifier(List<string> failures, string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            failures.Add($"{name} is required.");
        else if (value.Length > MaxIdentifierLength)
            failures.Add($"{name} cannot exceed {MaxIdentifierLength} characters.");
    }

    private static void ValidateTopology<TPeer>(List<string> failures, TopologyValidationArgs args, Func<TPeer, (string? NodeId, Uri? Uri)> readPeer, IReadOnlyList<TPeer> peers)
        where TPeer : notnull
    {
        ValidateIdentifier(failures, args.ClusterId, "ClusterId");
        ValidateIdentifier(failures, args.NodeId, "NodeId");
        ValidateUri(failures, args.NodeUri, "Uri");

        // Virtual node count bounds the consistent-hash ring size configured for this process.
        switch (args.VirtualNodes)
        {
            case <= 0:
                failures.Add("VirtualNodes must be greater than zero.");
                break;
            case > MaxVirtualNodes:
                failures.Add($"VirtualNodes cannot exceed {MaxVirtualNodes}.");
                break;
        }

        if (args is { PersistenceEnabled: false, DataDirectory: not null })
            failures.Add("DataDirectory requires persistence. Call UsePersistence() or pass --persist.");

        // Durability paths are validated only when persistence is enabled so in-memory nodes stay lightweight.
        if (args.PersistenceEnabled)
        {
            if (args.DataDirectory is { Length: > MaxDataDirectoryLength })
                failures.Add($"DataDirectory cannot exceed {MaxDataDirectoryLength} characters.");
            if (args.DataDirectory is not null && string.IsNullOrWhiteSpace(args.DataDirectory))
                failures.Add("DataDirectory cannot be empty or whitespace.");
        }

        if (peers.Count > MaxPeers)
            failures.Add($"Peers cannot contain more than {MaxPeers} entries.");

        ValidatePeers(failures, args.NodeId, args.NodeUri, readPeer, peers);
    }

    private static void ValidatePeers<TPeer>(List<string> failures, string? nodeId, Uri? nodeUri, Func<TPeer, (string? NodeId, Uri? Uri)> readPeer, IReadOnlyList<TPeer> peers)
        where TPeer : notnull
    {
        var peerIds = new HashSet<string>(StringComparer.Ordinal);
        var peerUris = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Empty peer list means single-node mode; otherwise the local node id must appear in Peers.
        var localNodePresent = peers.Count is 0;
        for (var i = 0; i < peers.Count; i++)
        {
            var peer = peers[i];
            var (peerNodeId, uri) = readPeer(peer);
            ValidateIdentifier(failures, peerNodeId, "Peers[].NodeId");
            ValidateUri(failures, uri, "Peers[].Uri");
            if (peerNodeId is not null && !peerIds.Add(peerNodeId))
                failures.Add($"Peers contains duplicate NodeId '{peerNodeId}'.");
            if (uri is not null)
            {
                var peerOrigin = uri.AbsoluteUri;
                if (!peerUris.Add(peerOrigin))
                    failures.Add($"Peers contains duplicate Uri '{peerOrigin}'.");
            }

            if (peerNodeId is null || nodeId is null || !string.Equals(peerNodeId, nodeId, StringComparison.Ordinal))
                continue;
            localNodePresent = true;

            // The self peer entry must advertise the same origin Uri as the local listener configuration.
            if (nodeUri is not null && uri is not null && !string.Equals(uri.AbsoluteUri, nodeUri.AbsoluteUri, StringComparison.OrdinalIgnoreCase))
            {
                failures.Add("Peers entry for the local NodeId must use the same Uri as Uri.");
            }
        }

        if (!localNodePresent)
            failures.Add("Peers must include the local NodeId.");
    }

    private static void ValidateUri(List<string> failures, Uri? value, string name)
    {
        if (value?.IsAbsoluteUri is not true || !string.Equals(value.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            failures.Add($"{name} must be an absolute https URI.");
            return;
        }

        if (value.OriginalString.Length > MaxUrlLength)
            failures.Add($"{name} cannot exceed {MaxUrlLength} characters.");
        if (string.IsNullOrWhiteSpace(value.Host))
            failures.Add($"{name} must include a host.");
        if (!string.IsNullOrEmpty(value.UserInfo) || !string.Equals(value.AbsolutePath, "/", StringComparison.OrdinalIgnoreCase) || !string.IsNullOrEmpty(value.Query) ||
            !string.IsNullOrEmpty(value.Fragment))
        {
            failures.Add($"{name} must be an origin URI without credentials, path, query, or fragment.");
        }
    }

    private sealed class TopologyValidationArgs
    {
        internal required string? ClusterId { get; init; }

        internal required string? NodeId { get; init; }

        internal required Uri? NodeUri { get; init; }

        internal required int VirtualNodes { get; init; }

        internal required bool PersistenceEnabled { get; init; }

        internal required string? DataDirectory { get; init; }
    }
}
