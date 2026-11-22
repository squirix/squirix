using System;

namespace Squirix.Internal.Cluster.Reliability;

/// <summary>Identifies the transport detail that suppresses retries after an ambiguous commit.</summary>
internal static class CommitOutcomeUnknownDetail
{
    internal const string Value = "COMMIT_OUTCOME_UNKNOWN";

    internal static bool Matches(string? detail) => string.Equals(detail, Value, StringComparison.Ordinal);
}
