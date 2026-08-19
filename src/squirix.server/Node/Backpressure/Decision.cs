using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Backpressure;

[Immutable]
internal readonly record struct Decision(bool IsAccepted, string? RejectReason)
{
    internal static Decision Accepted() => new(true, null);

    internal static Decision Rejected(string reason) => new(false, reason);
}
