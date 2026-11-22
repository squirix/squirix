using Squirix.Transport.Binary;

namespace Squirix.Transport;

/// <summary>Classifies the outcome of a remote cache operation.</summary>
public enum CacheTransportStatusCode
{
    /// <summary>The call succeeded.</summary>
    Ok = FrameStatus.Ok,

    /// <summary>The call was cancelled.</summary>
    Cancelled = FrameStatus.Cancelled,

    /// <summary>The failure was not classified.</summary>
    Unknown = FrameStatus.Unknown,

    /// <summary>A request argument was invalid.</summary>
    InvalidArgument = FrameStatus.InvalidArgument,

    /// <summary>The deadline expired.</summary>
    DeadlineExceeded = FrameStatus.DeadlineExceeded,

    /// <summary>The requested resource was absent.</summary>
    NotFound = FrameStatus.NotFound,

    /// <summary>The resource already existed.</summary>
    AlreadyExists = FrameStatus.AlreadyExists,

    /// <summary>The identity was forbidden from calling the operation.</summary>
    PermissionDenied = FrameStatus.PermissionDenied,

    /// <summary>The server could not admit the operation.</summary>
    ResourceExhausted = FrameStatus.ResourceExhausted,

    /// <summary>A required precondition did not hold.</summary>
    FailedPrecondition = FrameStatus.FailedPrecondition,

    /// <summary>The operation was aborted.</summary>
    Aborted = FrameStatus.Aborted,

    /// <summary>A request value exceeded its bounds.</summary>
    OutOfRange = FrameStatus.OutOfRange,

    /// <summary>The operation is not supported.</summary>
    Unimplemented = FrameStatus.Unimplemented,

    /// <summary>An internal error occurred.</summary>
    Internal = FrameStatus.Internal,

    /// <summary>The service is temporarily unavailable.</summary>
    Unavailable = FrameStatus.Unavailable,

    /// <summary>The response indicates unrecoverable data loss.</summary>
    DataLoss = FrameStatus.DataLoss,

    /// <summary>The endpoint rejected the credentials.</summary>
    Unauthenticated = FrameStatus.Unauthenticated,
}
