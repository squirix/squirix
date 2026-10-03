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
        Message = "Server client pool did not drain within the shutdown budget of {Budget}; peers still busy: {BusyPeers}. Their channels are disposed anyway")]
    internal static partial void ClientPoolDrainTimedOut(ILogger logger, TimeSpan budget, string busyPeers);

    [LoggerMessage(
        EventId = 5004,
        Level = LogLevel.Error,
        Message = "Cluster ring mismatch with peer {PeerNodeId} ({Direction}): local ring fingerprint {LocalFingerprint}, peer ring fingerprint {PeerFingerprint}. " +
                  "This node now refuses cache operations; make the peer lists agree on every node and restart the affected nodes")]
    internal static partial void RingMismatchDetected(ILogger logger, string peerNodeId, string direction, string localFingerprint, string peerFingerprint);
}
