namespace Squirix.Server.Node.Observability;

internal readonly record struct ReadinessResult(ReadinessStatus Status, string Description)
{
    internal static ReadinessResult Healthy(string description) => new(ReadinessStatus.Healthy, description);

    internal static ReadinessResult Degraded(string description) => new(ReadinessStatus.Degraded, description);

    internal static ReadinessResult Unhealthy(string description) => new(ReadinessStatus.Unhealthy, description);
}
