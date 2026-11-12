using System;
using Grpc.Core;
using Squirix.Internal.Cluster.Reliability;

namespace Squirix.Internal;

/// <summary>Recognizes the exact stable transport contract for an ambiguous durable commit.</summary>
internal static class CommitOutcomeUnknownClassifier
{
    internal static CommitOutcomeUnknownException? Map(RpcException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        var equals = exception.StatusCode is StatusCode.Unavailable && CommitOutcomeUnknownDetail.Matches(exception.Status.Detail);
        return equals ? new CommitOutcomeUnknownException(exception.Status.Detail, exception) : null;
    }
}
