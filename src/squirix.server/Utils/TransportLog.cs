using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for node transport.</summary>
internal static class TransportLog
{
    internal static void ClientPoolPolicyDisposeFailed(ILogger logger, Exception exception, string nodeId)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.ClientPoolPolicyDisposeFailedEvent.Write(logger, LogLevel.Debug, exception, ValueTuple.Create(nodeId));
    }

    internal static void ClientPoolChannelDisposeFailed(ILogger logger, Exception exception, string nodeId)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Events.ClientPoolChannelDisposeFailedEvent.Write(logger, LogLevel.Debug, exception, ValueTuple.Create(nodeId));
    }

    internal static void ClientPoolDrainTimedOut(ILogger logger, TimeSpan budget, int replicationCalls, string busyPeers)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ClientPoolDrainTimedOutEvent.Write(logger, LogLevel.Warning, null, (budget, replicationCalls, busyPeers));
    }

    internal static void ClientPoolMaterialLeaked(ILogger logger, int connections, TimeSpan budget)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.ClientPoolMaterialLeakedEvent.Write(logger, LogLevel.Error, null, (connections, budget));
    }

    internal static void ClientPoolMaterialReleasedLate(ILogger logger)
    {
        if (logger.IsEnabled(LogLevel.Information))
            Events.ClientPoolMaterialReleasedLateEvent.Write(logger, LogLevel.Information, null, ValueTuple.Create());
    }

    internal static void ClientPoolLateMaterialReleaseFailed(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.ClientPoolLateMaterialReleaseFailedEvent.Write(logger, LogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void ClientPoolLeaseCancelFailed(ILogger logger, Exception exception)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.ClientPoolLeaseCancelFailedEvent.Write(logger, LogLevel.Warning, exception, ValueTuple.Create());
    }

    internal static void RingMismatchDetected(ILogger logger, string peerNodeId, string direction, string localFingerprint, string peerFingerprint)
    {
        if (logger.IsEnabled(LogLevel.Error))
            Events.RingMismatchDetectedEvent.Write(logger, LogLevel.Error, null, (peerNodeId, direction, localFingerprint, peerFingerprint));
    }

    internal static void InboundPeerCertificateRefused(
        ILogger logger,
        string remoteEndpoint,
        string reason,
        string subject,
        string issuer,
        string thumbprint,
        TimeSpan relogInterval)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.InboundPeerCertificateRefusedEvent.Write(logger, LogLevel.Warning, null, (remoteEndpoint, reason, subject, issuer, thumbprint, relogInterval));
    }

    internal static void OutboundPeerCertificateRefused(
        ILogger logger,
        string peerNodeId,
        string reason,
        string subject,
        string issuer,
        string thumbprint,
        TimeSpan relogInterval)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.OutboundPeerCertificateRefusedEvent.Write(logger, LogLevel.Warning, null, (peerNodeId, reason, subject, issuer, thumbprint, relogInterval));
    }

    internal static void PeerCertificateRefusalsSuppressed(ILogger logger, int count, TimeSpan interval)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.PeerCertificateRefusalsSuppressedEvent.Write(logger, LogLevel.Warning, null, (count, interval));
    }

    internal static void PeerCertificateExpiredOnOpenConnection(ILogger logger, string peerNodeId, DateTimeOffset validUntil)
    {
        if (logger.IsEnabled(LogLevel.Warning))
            Events.CertificateExpiredOnOpenConnectionEvent.Write(logger, LogLevel.Warning, null, (peerNodeId, validUntil));
    }

    private static class Events
    {
        internal static readonly LogEvent ClientPoolPolicyDisposeFailedEvent = new(5001, nameof(TransportLog.ClientPoolPolicyDisposeFailed), "Failed to dispose server call policy for node {NodeId} during pool drain");

        internal static readonly LogEvent ClientPoolChannelDisposeFailedEvent = new(5002, nameof(TransportLog.ClientPoolChannelDisposeFailed), "Failed to dispose HTTP client for node {NodeId} during pool drain");

        internal static readonly LogEvent ClientPoolDrainTimedOutEvent = new(5003, nameof(TransportLog.ClientPoolDrainTimedOut), "Server client pool did not drain within the shutdown budget of {Budget}; replication calls still in flight: {ReplicationCalls}; peers still busy: {BusyPeers}. Their channels are disposed anyway");

        internal static readonly LogEvent ClientPoolMaterialLeakedEvent = new(5005, nameof(TransportLog.ClientPoolMaterialLeaked), "Server client pool transport material leaked: {Connections} connections were still open after the shutdown budget of {Budget}; the node certificate stays loaded instead of being freed under an open handshake");

        internal static readonly LogEvent ClientPoolMaterialReleasedLateEvent = new(5007, nameof(TransportLog.ClientPoolMaterialReleasedLate), "Server client pool released the transport material that was leaked at shutdown: every open connection has since closed");

        internal static readonly LogEvent ClientPoolLateMaterialReleaseFailedEvent = new(5008, nameof(TransportLog.ClientPoolLateMaterialReleaseFailed), "Server client pool could not wait for its leaked connections to close; the transport material stays loaded");

        internal static readonly LogEvent ClientPoolLeaseCancelFailedEvent = new(5006, nameof(TransportLog.ClientPoolLeaseCancelFailed), "A callback registered on a leased replication call threw while the server client pool cancelled its leases; disposal continues");

        internal static readonly LogEvent RingMismatchDetectedEvent = new(5004, nameof(TransportLog.RingMismatchDetected), "Cluster ring mismatch with peer {PeerNodeId} ({Direction}): local ring fingerprint {LocalFingerprint}, peer ring fingerprint {PeerFingerprint}. This node now refuses cache operations; make the peer lists agree on every node and restart the affected nodes");

        internal static readonly LogEvent InboundPeerCertificateRefusedEvent = new(5009, nameof(TransportLog.InboundPeerCertificateRefused), "Refused the internode mTLS certificate of an inbound peer connection from {RemoteEndpoint}: {Reason}. Certificate subject {Subject}, issuer {Issuer}, SHA-256 thumbprint {Thumbprint}. The refusal is logged again after {RelogInterval} or when its reason changes");

        internal static readonly LogEvent OutboundPeerCertificateRefusedEvent = new(5010, nameof(TransportLog.OutboundPeerCertificateRefused), "Refused the internode mTLS certificate presented by peer {PeerNodeId}: {Reason}. Certificate subject {Subject}, issuer {Issuer}, SHA-256 thumbprint {Thumbprint}. The refusal is logged again after {RelogInterval} or when its reason changes");

        internal static readonly LogEvent PeerCertificateRefusalsSuppressedEvent = new(5011, nameof(TransportLog.PeerCertificateRefusalsSuppressed), "Suppressed {Count} further internode mTLS certificate refusals in the last {Interval}");

        internal static readonly LogEvent CertificateExpiredOnOpenConnectionEvent = new(5012, nameof(TransportLog.PeerCertificateExpiredOnOpenConnection), "The internode mTLS certificate of peer {PeerNodeId} expired while its connection was open (its chain was valid until {ValidUntil}); calls on that connection are refused until the peer reconnects with a valid certificate");
    }
}
