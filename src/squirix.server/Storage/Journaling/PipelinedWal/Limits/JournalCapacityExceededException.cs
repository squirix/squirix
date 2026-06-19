using System;
using System.IO;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Limits;

/// <summary>Thrown when PipelinedWal segment roll/compaction cannot free enough capacity.</summary>
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
