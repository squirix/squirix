using System;
using Squirix.Server.Node.Backpressure;

namespace Squirix.Server;

/// <summary>
/// Configures node-level admission control (backpressure) for inbound cache requests.
/// Defaults keep node-wide limits on and leave per-client limits and rate limits off.
/// </summary>
/// <remarks>
/// Setting <see cref="PerClientMaxInFlight" /> or <see cref="PerClientRateLimitPerSecond" /> makes the node tell callers apart
/// by a backpressure client id: <c language="csharp">jwt:{subject}</c> for an authenticated principal,
/// <c language="csharp">conn:{connectionId}</c> for an HTTP request without a usable principal id, and
/// <c language="csharp">runtime</c> (one shared bucket) when there is no HTTP context.
/// </remarks>
public sealed class SquirixServerBackpressureOptions
{
    /// <summary>Gets or sets the node-wide maximum number of concurrently admitted requests. Default is <c language="csharp">256</c>; must be greater than zero.</summary>
    public int MaxInFlight { get; set; } = 256;

    /// <summary>Gets or sets the maximum number of queued requests. Default is <c language="csharp">128</c>; cannot be negative.</summary>
    public int MaxQueue { get; set; } = 128;

    /// <summary>Gets or sets the maximum time a request waits in the queue. Default is 250 milliseconds; must be greater than zero.</summary>
    public TimeSpan MaxQueueWait { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Gets or sets the maximum delay applied to a slowed-down request. Default is 25 milliseconds; cannot be negative.</summary>
    public TimeSpan MaxSlowdownDelay { get; set; } = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Gets or sets the node-wide rate limit burst size, or <see langword="null" /> for no node rate limit.
    /// Required when <see cref="NodeRateLimitPerSecond" /> is set and must be at least that rate.
    /// </summary>
    public int? NodeRateLimitBurst { get; set; }

    /// <summary>Gets or sets the node-wide rate limit in requests per second, or <see langword="null" /> for no node rate limit. Must be greater than zero when set.</summary>
    public int? NodeRateLimitPerSecond { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of concurrently admitted requests per client, or <see langword="null" /> for no per-client concurrency limit.
    /// Must be between 1 and <see cref="MaxInFlight" /> when set.
    /// </summary>
    public int? PerClientMaxInFlight { get; set; }

    /// <summary>Gets or sets the maximum number of queued requests per client, or <see langword="null" /> for no per-client queue limit. Cannot be negative when set.</summary>
    public int? PerClientMaxQueue { get; set; }

    /// <summary>
    /// Gets or sets the per-client rate limit burst size, or <see langword="null" /> for no per-client rate limit.
    /// Required when <see cref="PerClientRateLimitPerSecond" /> is set and must be at least that rate.
    /// </summary>
    public int? PerClientRateLimitBurst { get; set; }

    /// <summary>Gets or sets the per-client rate limit in requests per second, or <see langword="null" /> for no per-client rate limit. Must be greater than zero when set.</summary>
    public int? PerClientRateLimitPerSecond { get; set; }

    /// <summary>
    /// Gets or sets the in-flight count at which requests are rejected. Default is <c language="csharp">256</c>;
    /// must be between 1 and <see cref="MaxInFlight" /> and at least <see cref="SlowdownThreshold" />.
    /// </summary>
    public int RejectThreshold { get; set; } = 256;

    /// <summary>Gets or sets the in-flight count at which requests are slowed down. Default is <c language="csharp">192</c>; must be between 1 and <see cref="MaxInFlight" />.</summary>
    public int SlowdownThreshold { get; set; } = 192;

    internal AdmissionOptions ToAdmissionOptions() => new()
    {
        MaxInFlight = MaxInFlight,
        MaxQueue = MaxQueue,
        MaxQueueWait = MaxQueueWait,
        MaxSlowdownDelay = MaxSlowdownDelay,
        NodeRateLimitBurst = NodeRateLimitBurst,
        NodeRateLimitPerSecond = NodeRateLimitPerSecond,
        PerClientMaxInFlight = PerClientMaxInFlight,
        PerClientMaxQueue = PerClientMaxQueue,
        PerClientRateLimitBurst = PerClientRateLimitBurst,
        PerClientRateLimitPerSecond = PerClientRateLimitPerSecond,
        RejectThreshold = RejectThreshold,
        SlowdownThreshold = SlowdownThreshold,
    };
}
