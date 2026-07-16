namespace Squirix.Server.Node.Backpressure;

internal readonly record struct Decision(bool IsAccepted, string? RejectReason)
{
    internal static Decision Rejected(string reason) => new(false, reason);

    internal static Decision Accepted() => new(true, null);
}
