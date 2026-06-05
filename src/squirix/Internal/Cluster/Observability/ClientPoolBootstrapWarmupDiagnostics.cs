using System;
using System.Diagnostics;

namespace Squirix.Internal.Cluster.Observability;

/// <summary>
/// Structured diagnostics for bootstrap warm-up when some configured peers are skipped.
/// </summary>
internal static class ClientPoolBootstrapWarmupDiagnostics
{
    private static readonly ActivitySource ActivitySource = new("Squirix.Client", "1.0.0");

    /// <summary>
    /// Emits metrics and an activity event when warm-up succeeds on another peer but this peer failed.
    /// </summary>
    /// <param name="nodeId">Bootstrap peer node id that was skipped.</param>
    /// <param name="failure">Connection failure observed for the peer.</param>
    public static void RecordBootstrapPeerSkipped(string nodeId, Exception failure)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(failure);

        var reason = ClassifyReason(failure);
        ClientPoolMetrics.AddBootstrapWarmupSkipped(nodeId, reason);

        using var activity = ActivitySource.StartActivity("client.bootstrap.warmup.peer_skipped", ActivityKind.Client, default(ActivityContext));
        if (activity is null)
            return;

        _ = activity.SetTag("squirix.bootstrap.node_id", nodeId);
        _ = activity.SetTag("squirix.bootstrap.skip_reason", reason);
        _ = activity.SetTag("error.type", failure.GetType().FullName);
        _ = activity.SetStatus(ActivityStatusCode.Error, failure.Message);
    }

    private static string ClassifyReason(Exception failure) =>
        failure is InvalidOperationException && failure.Message.Contains("within", StringComparison.Ordinal) ? "connect_timeout" : "connect_failed";
}
