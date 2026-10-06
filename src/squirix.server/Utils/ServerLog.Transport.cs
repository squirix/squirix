using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <summary>Internode gRPC client pool drain and disposal logs, and cluster ring agreement logs.</summary>
internal static partial class ServerLog
{
    [LoggerMessage(EventId = 5001, Level = LogLevel.Debug, Message = "Failed to dispose server call policy for node {NodeId} during pool drain")]
    internal static partial void ClientPoolPolicyDisposeFailed(ILogger logger, Exception exception, string nodeId);

    [LoggerMessage(EventId = 5002, Level = LogLevel.Debug, Message = "Failed to dispose gRPC channel for node {NodeId} during pool drain")]
    internal static partial void ClientPoolChannelDisposeFailed(ILogger logger, Exception exception, string nodeId);

    [LoggerMessage(
        EventId = 5003,
        Level = LogLevel.Warning,
        Message = "Server client pool did not drain within the shutdown budget of {Budget}; replication calls still in flight: {ReplicationCalls}; " +
                  "peers still busy: {BusyPeers}. Their channels are disposed anyway")]
    internal static partial void ClientPoolDrainTimedOut(ILogger logger, TimeSpan budget, int replicationCalls, string busyPeers);

    [LoggerMessage(
        EventId = 5005,
        Level = LogLevel.Error,
        Message = "Server client pool transport material leaked: {Connections} connections were still open after the shutdown budget of {Budget}; " +
                  "the node certificate stays loaded instead of being freed under an open handshake")]
    internal static partial void ClientPoolMaterialLeaked(ILogger logger, int connections, TimeSpan budget);

    [LoggerMessage(
        EventId = 5007,
        Level = LogLevel.Information,
        Message = "Server client pool released the transport material that was leaked at shutdown: every open connection has since closed")]
    internal static partial void ClientPoolMaterialReleasedLate(ILogger logger);

    [LoggerMessage(
        EventId = 5008,
        Level = LogLevel.Error,
        Message = "Server client pool could not wait for its leaked connections to close; the transport material stays loaded")]
    internal static partial void ClientPoolLateMaterialReleaseFailed(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 5006,
        Level = LogLevel.Warning,
        Message = "A callback registered on a leased replication call threw while the server client pool cancelled its leases; disposal continues")]
    internal static partial void ClientPoolLeaseCancelFailed(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 5004,
        Level = LogLevel.Error,
        Message = "Cluster ring mismatch with peer {PeerNodeId} ({Direction}): local ring fingerprint {LocalFingerprint}, peer ring fingerprint {PeerFingerprint}. " +
                  "This node now refuses cache operations; make the peer lists agree on every node and restart the affected nodes")]
    internal static partial void RingMismatchDetected(ILogger logger, string peerNodeId, string direction, string localFingerprint, string peerFingerprint);
}
