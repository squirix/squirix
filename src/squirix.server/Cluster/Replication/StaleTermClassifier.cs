using System;
using System.Diagnostics.CodeAnalysis;
using Grpc.Core;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Classifies stale-term server responses that authorize at most one reroute to another endpoint.</summary>
[SuppressMessage("Usage", "MA0182:Internal type is apparently never used", Justification = "Test-only activation seam until failover activation wires the stale-term classifier in a follow-up milestone.")]
internal static class StaleTermClassifier
{
    /// <summary>Determines whether <paramref name="exception" /> reports a stale term.</summary>
    /// <param name="exception">The gRPC transport exception from the attempted endpoint.</param>
    /// <returns>The classification verdict for the response.</returns>
    internal static StaleTermVerdict Classify(RpcException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return Classify(exception.StatusCode, exception.Status.Detail);
    }

    /// <summary>Determines whether the status identifies a stale term.</summary>
    /// <param name="statusCode">The gRPC status code.</param>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <returns>The classification verdict for the status.</returns>
    internal static StaleTermVerdict Classify(StatusCode statusCode, string? detail) =>
        statusCode == StatusCode.FailedPrecondition && string.Equals(detail, RefusalCodes.StaleTerm, StringComparison.Ordinal)
            ? StaleTermVerdict.Stale
            : StaleTermVerdict.Current;
}
