using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using Squirix.Server.Errors;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster;

/// <summary>Tracks whether this node agrees with its peers on the ring and refuses cache operations once a mismatch is found.</summary>
/// <remarks>
/// The fence is one-way until restart: configurations that disagree on ownership never become consistent by themselves, so the node stays refused until it is restarted
/// with an agreeing configuration. All members are thread-safe.
/// </remarks>
internal sealed class RingAgreement
{
    private const string MissingFingerprint = "missing";

    private const string UnknownFingerprint = "unknown";

    private readonly RingFingerprint _local;
    private readonly ILogger<RingAgreement> _logger;
    private readonly HashSet<string> _reportedPeers = [with(StringComparer.Ordinal)];
    private readonly Lock _sync = new();
    private RingMismatchReport? _firstMismatch;
    private bool _fenced;

    internal RingAgreement(RingFingerprint local, ILogger<RingAgreement> logger)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(logger);
        _local = local;
        _logger = logger;
    }

    /// <summary>Gets the first mismatch this node detected, or <see langword="null" /> while it is not fenced.</summary>
    internal RingMismatchReport? FirstMismatch
    {
        get
        {
            lock (_sync)
                return _firstMismatch;
        }
    }

    /// <summary>Gets a value indicating whether this node detected a ring mismatch and refuses cache operations.</summary>
    internal bool IsFenced => Volatile.Read(ref _fenced);

    /// <summary>Throws when this node is fenced.</summary>
    /// <exception cref="Grpc.Core.RpcException">The ring-fenced refusal when this node detected a ring mismatch.</exception>
    internal void EnsureNotFenced()
    {
        if (IsFenced)
            throw RingMismatchFailure.Fenced();
    }

    /// <summary>Checks the ring fingerprint a peer sent with an internal owner call.</summary>
    /// <param name="peerFingerprint">The fingerprint header value, or <see langword="null" /> when the peer sent none.</param>
    /// <param name="peerNodeId">The calling peer node id.</param>
    /// <exception cref="Grpc.Core.RpcException">The ring-mismatch refusal when the fingerprint is missing or differs; this node is fenced.</exception>
    internal void EnsureInboundAgreement(string? peerFingerprint, string peerNodeId)
    {
        if (string.Equals(peerFingerprint, _local.Value, StringComparison.Ordinal))
            return;

        Record(peerNodeId, RingMismatchDirection.Inbound, peerFingerprint ?? MissingFingerprint);
        throw RingMismatchFailure.Mismatch();
    }

    /// <summary>Records that a key owner refused a forwarded call because its ring differs from the ring of this node.</summary>
    /// <param name="peerNodeId">The key owner node id.</param>
    internal void ReportOutboundMismatch(string peerNodeId) => Record(peerNodeId, RingMismatchDirection.Outbound, UnknownFingerprint);

    private void Record(string peerNodeId, RingMismatchDirection direction, string peerFingerprint)
    {
        bool firstForPeer;
        lock (_sync)
        {
            firstForPeer = _reportedPeers.Add(peerNodeId);
            if (_firstMismatch == null)
            {
                _firstMismatch = new RingMismatchReport(peerNodeId, direction, peerFingerprint);
                Volatile.Write(ref _fenced, true);
            }
        }

        if (firstForPeer)
            ServerLog.RingMismatchDetected(_logger, peerNodeId, direction == RingMismatchDirection.Inbound ? "Inbound" : "Outbound", _local.Value, peerFingerprint);
    }
}
