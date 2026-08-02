using System;
using System.Collections.Generic;

namespace Squirix.Server.Cluster;

internal static class TopologyValidator
{
    private const string ClusterIdRequired = "ClusterId is required.";
    private const string ClusterIdTooLong = "ClusterId cannot exceed 128 characters.";
    private const string DataDirectoryEmpty = "DataDirectory cannot be empty or whitespace.";
    private const string DataDirectoryRequiresPersistence = "DataDirectory requires persistence. Call UsePersistence() or pass --persist.";
    private const string DataDirectoryTooLong = "DataDirectory cannot exceed 1024 characters.";
    private const string LocalPeerUriMismatch = "Peers entry for the local NodeId must use the same Uri as Uri.";
    private const int MaxDataDirectoryLength = 1024;
    private const int MaxIdentifierLength = 128;
    private const int MaxPeers = 1024;
    private const int MaxUrlLength = 2048;
    private const int MaxVirtualNodes = 16384;
    private const string NodeIdRequired = "NodeId is required.";
    private const string NodeIdTooLong = "NodeId cannot exceed 128 characters.";
    private const string PeersDuplicateNodeId = "Peers contains duplicate NodeId.";
    private const string PeersMustIncludeLocalNodeId = "Peers must include the local NodeId.";
    private const string PeersNodeIdRequired = "Peers[].NodeId is required.";
    private const string PeersNodeIdTooLong = "Peers[].NodeId cannot exceed 128 characters.";
    private const string PeersTooMany = "Peers cannot contain more than 1024 entries.";
    private const string PeersUriDuplicate = "Peers contains duplicate Uri.";
    private const string PeersUriHostRequired = "Peers[].Uri must include a host.";
    private const string PeersUriHttpsRequired = "Peers[].Uri must be an absolute https URI.";
    private const string PeersUriOriginRequired = "Peers[].Uri must be an origin URI without credentials, path, query, or fragment.";
    private const string PeersUriTooLong = "Peers[].Uri cannot exceed 2048 characters.";
    private const string UriHostRequired = "Uri must include a host.";
    private const string UriHttpsRequired = "Uri must be an absolute https URI.";
    private const string UriOriginRequired = "Uri must be an origin URI without credentials, path, query, or fragment.";
    private const string UriTooLong = "Uri cannot exceed 2048 characters.";
    private const string VirtualNodesMustBePositive = "VirtualNodes must be greater than zero.";
    private const string VirtualNodesTooLarge = "VirtualNodes cannot exceed 16384.";

    private static readonly IReadOnlyList<string> NoValidationErrors = [];

    internal static bool TryValidate(TopologyOptions options, out IReadOnlyList<string> errors) => TryValidate(options, true, null, out errors);

    /// <summary>Validates topology fields with optional hosting persistence settings.</summary>
    /// <param name="options">Cluster topology options.</param>
    /// <param name="persistenceEnabled">Whether persistence is enabled for the host.</param>
    /// <param name="dataDirectory">Optional persistence data directory.</param>
    /// <param name="errors">Validation failures when the method returns <see langword="false" />.</param>
    /// <returns><see langword="true" /> when validation succeeds.</returns>
    internal static bool TryValidate(TopologyOptions options, bool persistenceEnabled, string? dataDirectory, out IReadOnlyList<string> errors)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();
        var args = new TopologyValidationArgs
        {
            ClusterId = options.ClusterId,
            NodeId = options.NodeId,
            NodeUri = options.Uri,
            VirtualNodes = options.VirtualNodes,
            PersistenceEnabled = persistenceEnabled,
            DataDirectory = dataDirectory,
        };
        ValidateTopology(failures, args, static peer => (peer.NodeId, peer.Uri), options.Peers);

        if (failures.Count is 0)
        {
            errors = NoValidationErrors;
            return true;
        }

        errors = failures;
        return false;
    }

    private static void ValidateIdentifier(List<string> failures, string? value, string requiredMessage, string tooLongMessage)
    {
        if (string.IsNullOrWhiteSpace(value))
            failures.Add(requiredMessage);
        else if (value.Length > MaxIdentifierLength)
            failures.Add(tooLongMessage);
    }

    private static bool ValidatePeerEntry(List<string> failures, string? nodeId, Uri? nodeUri, (string? NodeId, Uri? Uri) peer, HashSet<string> peerIds, HashSet<string> peerUris)
    {
        var (peerNodeId, uri) = peer;
        ValidateIdentifier(failures, peerNodeId, PeersNodeIdRequired, PeersNodeIdTooLong);
        ValidateUri(failures, uri, PeersUriHttpsRequired, PeersUriTooLong, PeersUriHostRequired, PeersUriOriginRequired);
        if (peerNodeId is not null && !peerIds.Add(peerNodeId))
            failures.Add(PeersDuplicateNodeId);
        if (uri is { IsAbsoluteUri: true } && !peerUris.Add(uri.AbsoluteUri))
            failures.Add(PeersUriDuplicate);

        if (peerNodeId is null || nodeId is null || !string.Equals(peerNodeId, nodeId, StringComparison.Ordinal))
            return false;

        // The self peer entry must advertise the same origin Uri as the local listener configuration.
        if (nodeUri is { IsAbsoluteUri: true } && uri is { IsAbsoluteUri: true } && !string.Equals(uri.AbsoluteUri, nodeUri.AbsoluteUri, StringComparison.OrdinalIgnoreCase))
            failures.Add(LocalPeerUriMismatch);

        return true;
    }

    private static void ValidatePeers<TPeer>(List<string> failures, string? nodeId, Uri? nodeUri, Func<TPeer, (string? NodeId, Uri? Uri)> readPeer, TPeer[] peers)
        where TPeer : notnull
    {
        var peerIds = new HashSet<string>(StringComparer.Ordinal);
        var peerUris = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Empty peer list means single-node mode; otherwise the local node id must appear in Peers.
        var localNodePresent = peers.Length is 0;
        for (var i = 0; i < peers.Length; i++)
            localNodePresent |= ValidatePeerEntry(failures, nodeId, nodeUri, readPeer(peers[i]), peerIds, peerUris);

        if (!localNodePresent)
            failures.Add(PeersMustIncludeLocalNodeId);
    }

    private static void ValidateTopology<TPeer>(List<string> failures, TopologyValidationArgs args, Func<TPeer, (string? NodeId, Uri? Uri)> readPeer, TPeer[] peers)
        where TPeer : notnull
    {
        ValidateIdentifier(failures, args.ClusterId, ClusterIdRequired, ClusterIdTooLong);
        ValidateIdentifier(failures, args.NodeId, NodeIdRequired, NodeIdTooLong);
        ValidateUri(failures, args.NodeUri, UriHttpsRequired, UriTooLong, UriHostRequired, UriOriginRequired);

        // Virtual node count bounds the consistent-hash ring size configured for this process.
        switch (args.VirtualNodes)
        {
            case <= 0:
                failures.Add(VirtualNodesMustBePositive);
                break;
            case > MaxVirtualNodes:
                failures.Add(VirtualNodesTooLarge);
                break;
        }

        if (args is { PersistenceEnabled: false, DataDirectory: not null })
            failures.Add(DataDirectoryRequiresPersistence);

        // Durability paths are validated only when persistence is enabled so in-memory nodes stay lightweight.
        if (args.PersistenceEnabled)
        {
            if (args.DataDirectory is { Length: > MaxDataDirectoryLength })
                failures.Add(DataDirectoryTooLong);
            if (args.DataDirectory is not null && string.IsNullOrWhiteSpace(args.DataDirectory))
                failures.Add(DataDirectoryEmpty);
        }

        if (peers.Length > MaxPeers)
            failures.Add(PeersTooMany);

        ValidatePeers(failures, args.NodeId, args.NodeUri, readPeer, peers);
    }

    private static void ValidateUri(List<string> failures, Uri? value, string httpsRequiredMessage, string tooLongMessage, string hostRequiredMessage, string originRequiredMessage)
    {
        if (value is null || !IsAbsoluteHttpsUri(value))
        {
            failures.Add(httpsRequiredMessage);
            return;
        }

        CollectHttpsUriFailures(failures, value, tooLongMessage, hostRequiredMessage, originRequiredMessage);
    }

    private static bool IsAbsoluteHttpsUri(Uri value) =>
        value.IsAbsoluteUri && string.Equals(value.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase);

    private static void CollectHttpsUriFailures(
        List<string> failures,
        Uri value,
        string tooLongMessage,
        string hostRequiredMessage,
        string originRequiredMessage)
    {
        if (value.OriginalString.Length > MaxUrlLength)
            failures.Add(tooLongMessage);
        if (string.IsNullOrWhiteSpace(value.Host))
            failures.Add(hostRequiredMessage);
        if (HasNonOriginParts(value))
            failures.Add(originRequiredMessage);
    }

    private static bool HasNonOriginParts(Uri value) =>
        !string.IsNullOrEmpty(value.UserInfo)
        || !string.Equals(value.AbsolutePath, "/", StringComparison.OrdinalIgnoreCase)
        || !string.IsNullOrEmpty(value.Query)
        || !string.IsNullOrEmpty(value.Fragment);

    private sealed class TopologyValidationArgs
    {
        internal required string? ClusterId { get; init; }

        internal required string? DataDirectory { get; init; }

        internal required string? NodeId { get; init; }

        internal required Uri? NodeUri { get; init; }

        internal required bool PersistenceEnabled { get; init; }

        internal required int VirtualNodes { get; init; }
    }
}
