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

    public static bool TryValidate(SquirixServerOptions options, out IReadOnlyList<string> errors)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();
        ValidateTopology(
            failures,
            options.ClusterId,
            options.NodeId,
            options.Url,
            options.VirtualNodes,
            options.DataDirectory,
            static peer => (peer.NodeId, peer.Url),
            options.Peers);

        if (failures.Count == 0)
        {
            errors = [];
            return true;
        }

        errors = failures;
        return false;
    }

    public static bool TryValidate(ClusterConfig options, out IReadOnlyList<string> errors)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();
        _ = Uri.TryCreate(options.Url, UriKind.Absolute, out var nodeUri);

        ValidateTopology(
            failures,
            options.ClusterId,
            options.NodeId,
            nodeUri,
            options.VirtualNodes,
            null,
            static peer =>
            {
                _ = Uri.TryCreate(peer.Url, UriKind.Absolute, out var peerUri);
                return (peer.NodeId, peerUri);
            },
            options.Peers);

        if (failures.Count == 0)
        {
            errors = [];
            return true;
        }

        errors = failures;
        return false;
    }

    public static void Validate(SquirixServerOptions options)
    {
        if (!TryValidate(options, out var errors))
            throw new ArgumentException(errors[0], nameof(options));
    }

    private static void ValidateIdentifier(List<string> failures, string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            failures.Add($"{name} is required.");
        else if (value.Length > MaxIdentifierLength)
            failures.Add($"{name} cannot exceed {MaxIdentifierLength} characters.");
    }

    private static void ValidateTopology<TPeer>(
        List<string> failures,
        string? clusterId,
        string? nodeId,
        Uri? nodeUrl,
        int virtualNodes,
        string? dataDirectory,
        Func<TPeer, (string? NodeId, Uri? Url)> readPeer,
        IReadOnlyList<TPeer> peers)
        where TPeer : notnull
    {
        ValidateIdentifier(failures, clusterId, "ClusterId");
        ValidateIdentifier(failures, nodeId, "NodeId");
        ValidateUrl(failures, nodeUrl, "Url");
        switch (virtualNodes)
        {
            case <= 0:
                failures.Add("VirtualNodes must be greater than zero.");
                break;
            case > MaxVirtualNodes:
                failures.Add($"VirtualNodes cannot exceed {MaxVirtualNodes}.");
                break;
        }

        if (dataDirectory is { Length: > MaxDataDirectoryLength })
            failures.Add($"DataDirectory cannot exceed {MaxDataDirectoryLength} characters.");
        if (dataDirectory is not null && string.IsNullOrWhiteSpace(dataDirectory))
            failures.Add("DataDirectory cannot be empty or whitespace.");

        if (peers.Count > MaxPeers)
            failures.Add($"Peers cannot contain more than {MaxPeers} entries.");

        var peerIds = new HashSet<string>(StringComparer.Ordinal);
        var peerUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var localNodePresent = peers.Count == 0;
        for (var i = 0; i < peers.Count; i++)
        {
            var peer = peers[i];
            var (peerNodeId, peerUrl) = readPeer(peer);
            ValidateIdentifier(failures, peerNodeId, "Peers[].NodeId");
            ValidateUrl(failures, peerUrl, "Peers[].Url");
            if (peerNodeId is not null && !peerIds.Add(peerNodeId))
                failures.Add($"Peers contains duplicate NodeId '{peerNodeId}'.");
            if (peerUrl is not null)
            {
                var peerOrigin = peerUrl.AbsoluteUri;
                if (!peerUrls.Add(peerOrigin))
                    failures.Add($"Peers contains duplicate Url '{peerOrigin}'.");
            }

            if (peerNodeId is null || nodeId is null || !string.Equals(peerNodeId, nodeId, StringComparison.Ordinal))
                continue;
            localNodePresent = true;
            if (nodeUrl is not null && peerUrl is not null && !string.Equals(peerUrl.AbsoluteUri, nodeUrl.AbsoluteUri, StringComparison.OrdinalIgnoreCase))
            {
                failures.Add("Peers entry for the local NodeId must use the same Url as Url.");
            }
        }

        if (!localNodePresent)
            failures.Add("Peers must include the local NodeId.");
    }

    private static void ValidateUrl(List<string> failures, Uri? value, string name)
    {
        if (value is null || !value.IsAbsoluteUri || value.Scheme != Uri.UriSchemeHttps)
        {
            failures.Add($"{name} must be an absolute https URI.");
            return;
        }

        if (value.OriginalString.Length > MaxUrlLength)
            failures.Add($"{name} cannot exceed {MaxUrlLength} characters.");
        if (string.IsNullOrWhiteSpace(value.Host))
            failures.Add($"{name} must include a host.");
        if (!string.IsNullOrEmpty(value.UserInfo) || value.AbsolutePath != "/" || !string.IsNullOrEmpty(value.Query) || !string.IsNullOrEmpty(value.Fragment))
            failures.Add($"{name} must be an origin URI without credentials, path, query, or fragment.");
    }
}
