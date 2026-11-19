using Squirix.Transport.Binary;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Binary response codes inspected by black-box tests.</summary>
internal enum WireStatus
{
    /// <inheritdoc cref="FrameStatus.Ok" />
    Ok = FrameStatus.Ok,

    /// <inheritdoc cref="FrameStatus.Cancelled" />
    Cancelled = FrameStatus.Cancelled,

    /// <inheritdoc cref="FrameStatus.Unknown" />
    Unknown = FrameStatus.Unknown,

    /// <inheritdoc cref="FrameStatus.DeadlineExceeded" />
    DeadlineExceeded = FrameStatus.DeadlineExceeded,

    /// <inheritdoc cref="FrameStatus.ResourceExhausted" />
    ResourceExhausted = FrameStatus.ResourceExhausted,

    /// <inheritdoc cref="FrameStatus.FailedPrecondition" />
    FailedPrecondition = FrameStatus.FailedPrecondition,

    /// <inheritdoc cref="FrameStatus.Internal" />
    Internal = FrameStatus.Internal,

    /// <inheritdoc cref="FrameStatus.Unavailable" />
    Unavailable = FrameStatus.Unavailable,
}
