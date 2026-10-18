using System;
using System.IO;
using JetBrains.Annotations;

namespace Squirix.Server.Storage.Replication;

/// <summary>
/// Thrown when an append to the group log failed after its frames were handed to the storage: the frames may be on disk and a restart can
/// recover them as an uncommitted tail, so the append is neither confirmed nor refused. The log is latched failed until the node restarts.
/// </summary>
[UsedImplicitly]
internal sealed class GroupLogAppendUnconfirmedException : IOException
{
    private const string ExceptionMessage = "The group log append is unconfirmed: its frames may be on disk, and the log is failed until the node restarts.";

    /// <summary>Initializes a new instance of the <see cref="GroupLogAppendUnconfirmedException" /> class.</summary>
    internal GroupLogAppendUnconfirmedException()
        : base(ExceptionMessage)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GroupLogAppendUnconfirmedException" /> class with a message.</summary>
    /// <param name="message">The exception message.</param>
    internal GroupLogAppendUnconfirmedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GroupLogAppendUnconfirmedException" /> class with the failure that left the append unconfirmed.</summary>
    /// <param name="innerException">The failure of the write, the flush, or the index update.</param>
    internal GroupLogAppendUnconfirmedException(Exception innerException)
        : base(ExceptionMessage, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GroupLogAppendUnconfirmedException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal GroupLogAppendUnconfirmedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GroupLogAppendUnconfirmedException" /> class with a message and an error code.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="hresult">The error code of the failure.</param>
    internal GroupLogAppendUnconfirmedException(string message, int hresult)
        : base(message, hresult)
    {
    }
}
