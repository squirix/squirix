using System;
using System.Text.Json.Serialization;
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
[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed class SquirixServerBackpressureOptions
{
    /// <summary>
    /// Gets or sets the node-wide maximum number of concurrently admitted requests. Default is <c language="csharp">256</c>; must be greater than zero.
    /// When all slots are taken, further requests wait in the queue (see <see cref="MaxQueue" />) and are rejected once it is full.
    /// </summary>
    public int MaxInFlight { get; set; } = 256;

    /// <summary>
    /// Gets or sets the maximum number of requests waiting in the queue for a free slot, served in arrival order.
    /// Default is <c language="csharp">128</c>; cannot be negative. A request arriving at a full queue is rejected immediately.
    /// </summary>
    public int MaxQueue { get; set; } = 128;

    /// <summary>
    /// Gets or sets the maximum time a request waits in the queue for a slot, after any slowdown delay. Default is 250 milliseconds
    /// ("00:00:00.250" in settings); must be greater than zero and cannot exceed one minute.
    /// </summary>
    public TimeSpan MaxQueueWait { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Gets or sets the maximum delay applied to a slowed-down request. Default is 25 milliseconds ("00:00:00.025" in settings);
    /// cannot be negative or exceed 5 seconds.
    /// </summary>
    public TimeSpan MaxSlowdownDelay { get; set; } = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// Gets or sets the node-wide rate limit burst size, or <see langword="null" /> for no node rate limit.
    /// A burst needs <see cref="NodeRateLimitPerSecond" />: it is required when the rate is set, must be at least that rate,
    /// and is rejected without it.
    /// </summary>
    public int? NodeRateLimitBurst { get; set; }

    /// <summary>
    /// Gets or sets the node-wide rate limit in requests per second, or <see langword="null" /> for no node rate limit.
    /// Must be greater than zero when set.
    /// </summary>
    public int? NodeRateLimitPerSecond { get; set; }

    /// <summary>
    /// Gets or sets the maximum number of requests per client that are admitted or waiting in the queue, or <see langword="null" /> for no per-client concurrency limit.
    /// Must be between 1 and <see cref="MaxInFlight" /> when set.
    /// </summary>
    public int? PerClientMaxInFlight { get; set; }

    /// <summary>
    /// Gets or sets the per-client rate limit burst size, or <see langword="null" /> for no per-client rate limit.
    /// A burst needs <see cref="PerClientRateLimitPerSecond" />: it is required when the rate is set, must be at least that rate,
    /// and is rejected without it.
    /// </summary>
    public int? PerClientRateLimitBurst { get; set; }

    /// <summary>
    /// Gets or sets the per-client rate limit in requests per second, or <see langword="null" /> for no per-client rate limit.
    /// Must be greater than zero when set.
    /// </summary>
    public int? PerClientRateLimitPerSecond { get; set; }

    /// <summary>
    /// Gets or sets the in-flight count at which requests are slowed down. Default is <c language="csharp">192</c>;
    /// must be between 1 and <see cref="MaxInFlight" />.
    /// </summary>
    /// <remarks>
    /// The delay grows linearly from near zero at this count to <see cref="MaxSlowdownDelay" /> when <see cref="MaxInFlight" /> slots are taken.
    /// A request that is slowed down and then queued can wait up to <see cref="MaxSlowdownDelay" /> plus <see cref="MaxQueueWait" />.
    /// </remarks>
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
        PerClientRateLimitBurst = PerClientRateLimitBurst,
        PerClientRateLimitPerSecond = PerClientRateLimitPerSecond,
        SlowdownThreshold = SlowdownThreshold,
    };
}
