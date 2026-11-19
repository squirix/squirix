namespace Squirix.Transport.Binary;

/// <summary>Classifies the outcome of a remote cache operation.</summary>
internal enum FrameStatus
{
    /// <summary>The call succeeded.</summary>
    Ok = 0,

    /// <summary>The call was cancelled.</summary>
    Cancelled = 1,

    /// <summary>The failure was not classified.</summary>
    Unknown = 2,

    /// <summary>A request argument was invalid.</summary>
    InvalidArgument = 3,

    /// <summary>The deadline expired.</summary>
    DeadlineExceeded = 4,

    /// <summary>The requested resource was absent.</summary>
    NotFound = 5,

    /// <summary>The resource already existed.</summary>
    AlreadyExists = 6,

    /// <summary>The identity was forbidden from calling the operation.</summary>
    PermissionDenied = 7,

    /// <summary>The server could not admit the operation.</summary>
    ResourceExhausted = 8,

    /// <summary>A required precondition did not hold.</summary>
    FailedPrecondition = 9,

    /// <summary>The operation was aborted.</summary>
    Aborted = 10,

    /// <summary>A request value exceeded its bounds.</summary>
    OutOfRange = 11,

    /// <summary>The operation is not supported.</summary>
    Unimplemented = 12,

    /// <summary>An internal error occurred.</summary>
    Internal = 13,

    /// <summary>The service is temporarily unavailable.</summary>
    Unavailable = 14,

    /// <summary>The response indicates unrecoverable data loss.</summary>
    DataLoss = 15,

    /// <summary>The endpoint rejected the credentials.</summary>
    Unauthenticated = 16,
}
