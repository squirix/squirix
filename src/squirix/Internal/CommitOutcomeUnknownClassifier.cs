using System;
using Grpc.Core;

namespace Squirix.Internal;

/// <summary>Recognizes the exact stable transport contract for an ambiguous durable commit.</summary>
internal static class CommitOutcomeUnknownClassifier
{
    internal static CommitOutcomeUnknownException? Map(RpcException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception.StatusCode is StatusCode.Unavailable && string.Equals(exception.Status.Detail, CommitOutcomeUnknownException.StableDetail, StringComparison.Ordinal)
            ? new CommitOutcomeUnknownException(exception.Status.Detail, exception) : null;
    }
}
