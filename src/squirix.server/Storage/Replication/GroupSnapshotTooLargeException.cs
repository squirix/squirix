using System;

namespace Squirix.Server.Storage.Replication;

/// <summary>Thrown when a replica group snapshot would exceed the configured maximum snapshot size; nothing was written.</summary>
internal sealed class GroupSnapshotTooLargeException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="GroupSnapshotTooLargeException" /> class.</summary>
    public GroupSnapshotTooLargeException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GroupSnapshotTooLargeException" /> class with a message.</summary>
    /// <param name="message">The exception message.</param>
    public GroupSnapshotTooLargeException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GroupSnapshotTooLargeException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public GroupSnapshotTooLargeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
