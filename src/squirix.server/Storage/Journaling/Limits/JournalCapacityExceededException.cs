using System;
using JetBrains.Annotations;

namespace Squirix.Server.Storage.Journaling.Limits;

/// <summary>Thrown when Pipelined segment roll/compaction cannot free enough capacity.</summary>
[UsedImplicitly]
internal sealed class JournalCapacityExceededException : InvalidOperationException
{
    public JournalCapacityExceededException()
    {
    }

    public JournalCapacityExceededException(string message)
        : base(message)
    {
    }

    public JournalCapacityExceededException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
