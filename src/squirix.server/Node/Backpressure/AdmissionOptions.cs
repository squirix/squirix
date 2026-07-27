using System;

namespace Squirix.Server.Node.Backpressure;

/// <summary>Configures node-level admission control for inbound gRPC cache requests.</summary>
internal sealed record AdmissionOptions
{
    internal bool Enabled { get; init; } = true;

    internal int MaxInFlight { get; init; } = 256;

    internal int MaxQueue { get; init; } = 128;

    internal TimeSpan MaxQueueWait { get; init; } = TimeSpan.FromMilliseconds(250);

    internal TimeSpan MaxSlowdownDelay { get; init; } = TimeSpan.FromMilliseconds(25);

    internal int? NodeRateLimitBurst { get; init; }

    internal int? NodeRateLimitPerSecond { get; init; }

    internal int? PerClientMaxInFlight { get; init; }

    internal int? PerClientMaxQueue { get; init; }

    internal int? PerClientRateLimitBurst { get; init; }

    internal int? PerClientRateLimitPerSecond { get; init; }

    internal int RejectThreshold { get; init; } = 256;

    internal int SlowdownThreshold { get; init; } = 192;

    internal void Validate()
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

        ValidateNodeRateLimit();
        ValidatePerClientRateLimit();
    }

    private static void ValidateRateLimit(int? rate, int? burst, string rateRequiredMessage, string burstRequiredMessage, string burstGteRateMessage)
    {
        if (rate is not null)
        {
            if (rate.Value <= 0)
                throw new InvalidOperationException(rateRequiredMessage);

            if (burst is null)
                throw new InvalidOperationException(burstRequiredMessage);

            var configuredBurst = burst.Value;
            if (configuredBurst <= 0)
                throw new InvalidOperationException(burstRequiredMessage);

            if (configuredBurst < rate.Value)
                throw new InvalidOperationException(burstGteRateMessage);
        }
        else if (burst is not null)
        {
            throw new InvalidOperationException(rateRequiredMessage);
        }
    }

    private void ValidateNodeRateLimit()
    {
        ValidateRateLimit(
            NodeRateLimitPerSecond,
            NodeRateLimitBurst,
            "Backpressure NodeRateLimitPerSecond must be greater than zero when configured.",
            "Backpressure NodeRateLimitBurst must be greater than zero when configured.",
            "Backpressure NodeRateLimitBurst must be greater than or equal to NodeRateLimitPerSecond.");
    }

    private void ValidatePerClientRateLimit()
    {
        ValidateRateLimit(
            PerClientRateLimitPerSecond,
            PerClientRateLimitBurst,
            "Backpressure PerClientRateLimitPerSecond must be greater than zero when configured.",
            "Backpressure PerClientRateLimitBurst must be greater than zero when configured.",
            "Backpressure PerClientRateLimitBurst must be greater than or equal to PerClientRateLimitPerSecond.");
    }
}
