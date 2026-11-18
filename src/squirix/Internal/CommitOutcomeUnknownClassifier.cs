using System;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Transport;

namespace Squirix.Internal;

/// <summary>Recognizes the exact stable transport contract for an ambiguous durable commit.</summary>
internal static class CommitOutcomeUnknownClassifier
{
    internal static CommitOutcomeUnknownException? Map(SquirixTransportException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        var equals = exception.StatusCode is CacheTransportStatusCode.Unavailable && CommitOutcomeUnknownDetail.Matches(exception.Status.Detail);
        return equals ? new CommitOutcomeUnknownException(exception.Status.Detail, exception) : null;
    }
}
