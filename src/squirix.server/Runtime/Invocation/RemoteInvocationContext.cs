using System.Threading;

namespace Squirix.Server.Runtime.Invocation;

/// <summary>Async-local remote-invocation classification used by server endpoint handlers.</summary>
internal static class RemoteInvocationContext
{
    private static readonly object InternalOwnerMarker = new();
    private static readonly object UnknownMutationOutcomeMarker = new();
    private static readonly AsyncLocal<object?> Invocation = new();

    internal static bool IsInternalOwnerInvocation => ReferenceEquals(Invocation.Value, InternalOwnerMarker);
    internal static bool ReportsUnknownMutationOutcome => ReferenceEquals(Invocation.Value, UnknownMutationOutcomeMarker);

    internal static RemoteInvocationScope EnterRemoteInvocation(bool isInternalOwnerInvocation = false, bool reportUnknownMutationOutcome = false)
    {
        var previous = new RemoteInvocationScope(IsInternalOwnerInvocation, ReportsUnknownMutationOutcome);
        RestoreInvocation(isInternalOwnerInvocation, reportUnknownMutationOutcome);
        return previous;
    }

    internal static void RestoreInvocation(bool internalOwner, bool reportsUnknownMutationOutcome)
    {
        var next = (internalOwner, reportsUnknownMutationOutcome) switch
        {
            (true, _) => InternalOwnerMarker,
            (_, true) => UnknownMutationOutcomeMarker,
            _ => null,
        };
        if (!ReferenceEquals(Invocation.Value, next))
            Invocation.Value = next;
    }
}
