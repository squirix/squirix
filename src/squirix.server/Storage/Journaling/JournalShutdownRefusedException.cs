using System;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// The journal refused work, or faulted a waiter, because shutdown began. It is the single marker of a failure the shutdown itself caused, so
/// a stop can tell it from a data failure without matching on names.
/// </summary>
#pragma warning disable RCS1194 // ObjectDisposedException has no parameterless constructor to forward to, so the standard set cannot be matched exactly.
internal sealed class JournalShutdownRefusedException : ObjectDisposedException
{
    /// <summary>Initializes a new instance of the <see cref="JournalShutdownRefusedException" /> class.</summary>
    internal JournalShutdownRefusedException()
        : base(nameof(JournalCoordinator))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JournalShutdownRefusedException" /> class with the refusing component.</summary>
    /// <param name="message">The name of the component that refused the work, reported as the object name.</param>
    internal JournalShutdownRefusedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JournalShutdownRefusedException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The exception that caused the refusal.</param>
    internal JournalShutdownRefusedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
#pragma warning restore RCS1194
