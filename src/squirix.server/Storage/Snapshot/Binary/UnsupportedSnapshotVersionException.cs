using System;
using JetBrains.Annotations;

namespace Squirix.Server.Storage.Snapshot.Binary;

/// <summary>Thrown when a snapshot file carries the snapshot magic under a format version this build does not read; recovery must stop instead of replaying the journal without it.</summary>
[UsedImplicitly]
internal sealed class UnsupportedSnapshotVersionException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="UnsupportedSnapshotVersionException" /> class.</summary>
    internal UnsupportedSnapshotVersionException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="UnsupportedSnapshotVersionException" /> class with a message.</summary>
    /// <param name="message">The exception message.</param>
    internal UnsupportedSnapshotVersionException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="UnsupportedSnapshotVersionException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal UnsupportedSnapshotVersionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
