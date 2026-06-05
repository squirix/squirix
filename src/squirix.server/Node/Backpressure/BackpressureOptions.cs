using System;

namespace Squirix.Server.Node.Backpressure;

/// <summary>
/// Configures node-level admission control for inbound REST and gRPC requests.
/// </summary>
internal sealed record BackpressureOptions
{
    public bool Enabled { get; init; } = true;

    public int MaxInFlight { get; init; } = 256;

    public int MaxQueue { get; init; } = 128;

    public TimeSpan MaxQueueWait { get; init; } = TimeSpan.FromMilliseconds(250);

    public TimeSpan MaxSlowdownDelay { get; init; } = TimeSpan.FromMilliseconds(25);

    public int? NodeRateLimitBurst { get; init; }

    public int? NodeRateLimitPerSecond { get; init; }

    public int? PerClientMaxInFlight { get; init; }

    public int? PerClientMaxQueue { get; init; }

    public int? PerClientRateLimitBurst { get; init; }

    public int? PerClientRateLimitPerSecond { get; init; }

    public int RejectThreshold { get; init; } = 256;

    public int SlowdownThreshold { get; init; } = 192;

    public void Validate()
    {
        if (MaxInFlight <= 0)
            throw new InvalidOperationException("Backpressure MaxInFlight must be greater than zero.");

        if (MaxQueue < 0)
            throw new InvalidOperationException("Backpressure MaxQueue cannot be negative.");

        if (PerClientMaxInFlight is <= 0)
            throw new InvalidOperationException("Backpressure PerClientMaxInFlight must be greater than zero when configured.");

        if (PerClientMaxInFlight > MaxInFlight)
            throw new InvalidOperationException("Backpressure PerClientMaxInFlight cannot exceed MaxInFlight.");

        if (PerClientMaxQueue < 0)
            throw new InvalidOperationException("Backpressure PerClientMaxQueue cannot be negative.");

        if (SlowdownThreshold <= 0 || SlowdownThreshold > MaxInFlight)
            throw new InvalidOperationException("Backpressure SlowdownThreshold must be in the range [1, MaxInFlight].");

        if (RejectThreshold <= 0 || RejectThreshold > MaxInFlight)
            throw new InvalidOperationException("Backpressure RejectThreshold must be in the range [1, MaxInFlight].");

        if (RejectThreshold < SlowdownThreshold)
            throw new InvalidOperationException("Backpressure RejectThreshold must be greater than or equal to SlowdownThreshold.");

        if (MaxSlowdownDelay < TimeSpan.Zero)
            throw new InvalidOperationException("Backpressure MaxSlowdownDelay cannot be negative.");

        if (MaxQueueWait <= TimeSpan.Zero)
            throw new InvalidOperationException("Backpressure MaxQueueWait must be greater than zero.");

        ValidateRateLimit("NodeRateLimitPerSecond", NodeRateLimitPerSecond, NodeRateLimitBurst);
        ValidateRateLimit("PerClientRateLimitPerSecond", PerClientRateLimitPerSecond, PerClientRateLimitBurst);
    }

    private static void ValidateRateLimit(string rateName, int? rate, int? burst)
    {
        if (rate.HasValue)
        {
            var burstName = rateName.Replace("PerSecond", "Burst", StringComparison.Ordinal);
            if (rate.Value <= 0)
                throw new InvalidOperationException($"Backpressure {rateName} must be greater than zero when configured.");

            if (!burst.HasValue)
                throw new InvalidOperationException($"Backpressure {burstName} must be greater than zero when configured.");

            var configuredBurst = burst.Value;
            if (configuredBurst <= 0)
                throw new InvalidOperationException($"Backpressure {burstName} must be greater than zero when configured.");

            if (configuredBurst < rate.Value)
                throw new InvalidOperationException($"Backpressure {burstName} must be greater than or equal to {rateName}.");
        }
        else
        {
            if (burst.HasValue)
                throw new InvalidOperationException($"Backpressure {rateName} must be greater than zero when configured.");
        }
    }
}
