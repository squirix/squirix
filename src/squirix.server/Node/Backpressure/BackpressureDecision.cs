namespace Squirix.Server.Node.Backpressure;

internal readonly record struct BackpressureDecision(bool IsAccepted, string? RejectReason)
{
    public static BackpressureDecision Accepted() => new(true, null);

    public static BackpressureDecision Rejected(string reason) => new(false, reason);
}
