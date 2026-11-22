using System;
using Squirix.Server.Logging;

namespace Squirix.Server.Utils;

/// <summary>Diagnostic events for node transport.</summary>
internal static class TransportLog
{
    internal static void ClientPoolPolicyDisposeFailed(IServerLogger logger, Exception exception, string nodeId)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ClientPoolPolicyDisposeFailedEvent.Write(logger, ServerLogLevel.Debug, exception, ValueTuple.Create(nodeId));
    }

    internal static void ClientPoolChannelDisposeFailed(IServerLogger logger, Exception exception, string nodeId)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.ClientPoolChannelDisposeFailedEvent.Write(logger, ServerLogLevel.Debug, exception, ValueTuple.Create(nodeId));
    }

    internal static void ClientPoolDrainTimedOut(IServerLogger logger, TimeSpan budget, int replicationCalls, string busyPeers)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ClientPoolDrainTimedOutEvent.Write(logger, ServerLogLevel.Warning, null, (budget, replicationCalls, busyPeers));
    }

    internal static void ClientPoolMaterialLeaked(IServerLogger logger, int connections, TimeSpan budget)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.ClientPoolMaterialLeakedEvent.Write(logger, ServerLogLevel.Error, null, (connections, budget));
    }

    internal static void ClientPoolMaterialReleasedLate(IServerLogger logger)
    {
        if (logger.IsEnabled(ServerLogLevel.Information))
            Events.ClientPoolMaterialReleasedLateEvent.Write(logger, ServerLogLevel.Information, null, ValueTuple.Create());
    }

    internal static void ClientPoolLateMaterialReleaseFailed(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.ClientPoolLateMaterialReleaseFailedEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create());
    }

    internal static void ClientPoolLeaseCancelFailed(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.ClientPoolLeaseCancelFailedEvent.Write(logger, ServerLogLevel.Warning, exception, ValueTuple.Create());
    }

    internal static void RingMismatchDetected(IServerLogger logger, string peerNodeId, string direction, string localFingerprint, string peerFingerprint)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.RingMismatchDetectedEvent.Write(logger, ServerLogLevel.Error, null, (peerNodeId, direction, localFingerprint, peerFingerprint));
    }

    internal static void InboundPeerCertificateRefused(
        IServerLogger logger,
        string remoteEndpoint,
        string reason,
        string subject,
        string issuer,
        string thumbprint,
        TimeSpan relogInterval)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.InboundPeerCertificateRefusedEvent.Write(logger, ServerLogLevel.Warning, null, (remoteEndpoint, reason, subject, issuer, thumbprint, relogInterval));
    }

    internal static void OutboundPeerCertificateRefused(
        IServerLogger logger,
        string peerNodeId,
        string reason,
        string subject,
        string issuer,
        string thumbprint,
        TimeSpan relogInterval)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.OutboundPeerCertificateRefusedEvent.Write(logger, ServerLogLevel.Warning, null, (peerNodeId, reason, subject, issuer, thumbprint, relogInterval));
    }

    internal static void PeerCertificateRefusalsSuppressed(IServerLogger logger, int count, TimeSpan interval)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.PeerCertificateRefusalsSuppressedEvent.Write(logger, ServerLogLevel.Warning, null, (count, interval));
    }

    internal static void PeerCertificateExpiredOnOpenConnection(IServerLogger logger, string peerNodeId, DateTimeOffset validUntil)
    {
        if (logger.IsEnabled(ServerLogLevel.Warning))
            Events.CertificateExpiredOnOpenConnectionEvent.Write(logger, ServerLogLevel.Warning, null, (peerNodeId, validUntil));
    }

    private static class Events
    {
        internal static readonly LogEvent ClientPoolPolicyDisposeFailedEvent = new(5001, nameof(ClientPoolPolicyDisposeFailed), "Failed to dispose server call policy for node {NodeId} during pool drain");

        internal static readonly LogEvent ClientPoolChannelDisposeFailedEvent = new(5002, nameof(ClientPoolChannelDisposeFailed), "Failed to dispose HTTP client for node {NodeId} during pool drain");

        internal static readonly LogEvent ClientPoolDrainTimedOutEvent = new(5003, nameof(ClientPoolDrainTimedOut), "Server client pool did not drain within the shutdown budget of {Budget}; replication calls still in flight: {ReplicationCalls}; peers still busy: {BusyPeers}. Their channels are disposed anyway");

        internal static readonly LogEvent ClientPoolMaterialLeakedEvent = new(5005, nameof(ClientPoolMaterialLeaked), "Server client pool transport material leaked: {Connections} connections were still open after the shutdown budget of {Budget}; the node certificate stays loaded instead of being freed under an open handshake");

        internal static readonly LogEvent ClientPoolMaterialReleasedLateEvent = new(5007, nameof(ClientPoolMaterialReleasedLate), "Server client pool released the transport material that was leaked at shutdown: every open connection has since closed");

        internal static readonly LogEvent ClientPoolLateMaterialReleaseFailedEvent = new(5008, nameof(ClientPoolLateMaterialReleaseFailed), "Server client pool could not wait for its leaked connections to close; the transport material stays loaded");

        internal static readonly LogEvent ClientPoolLeaseCancelFailedEvent = new(5006, nameof(ClientPoolLeaseCancelFailed), "A callback registered on a leased replication call threw while the server client pool cancelled its leases; disposal continues");

        internal static readonly LogEvent RingMismatchDetectedEvent = new(5004, nameof(RingMismatchDetected), "Cluster ring mismatch with peer {PeerNodeId} ({Direction}): local ring fingerprint {LocalFingerprint}, peer ring fingerprint {PeerFingerprint}. This node now refuses cache operations; make the peer lists agree on every node and restart the affected nodes");

        internal static readonly LogEvent InboundPeerCertificateRefusedEvent = new(5009, nameof(InboundPeerCertificateRefused), "Refused the internode mTLS certificate of an inbound peer connection from {RemoteEndpoint}: {Reason}. Certificate subject {Subject}, issuer {Issuer}, SHA-256 thumbprint {Thumbprint}. The refusal is logged again after {RelogInterval} or when its reason changes");

        internal static readonly LogEvent OutboundPeerCertificateRefusedEvent = new(5010, nameof(OutboundPeerCertificateRefused), "Refused the internode mTLS certificate presented by peer {PeerNodeId}: {Reason}. Certificate subject {Subject}, issuer {Issuer}, SHA-256 thumbprint {Thumbprint}. The refusal is logged again after {RelogInterval} or when its reason changes");

        internal static readonly LogEvent PeerCertificateRefusalsSuppressedEvent = new(5011, nameof(PeerCertificateRefusalsSuppressed), "Suppressed {Count} further internode mTLS certificate refusals in the last {Interval}");

        internal static readonly LogEvent CertificateExpiredOnOpenConnectionEvent = new(5012, nameof(PeerCertificateExpiredOnOpenConnection), "The internode mTLS certificate of peer {PeerNodeId} expired while its connection was open (its chain was valid until {ValidUntil}); calls on that connection are refused until the peer reconnects with a valid certificate");
    }
}
