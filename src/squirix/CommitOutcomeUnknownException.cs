using System;
using JetBrains.Annotations;

namespace Squirix;

/// <summary>Thrown when a mutation may have committed but its exact durable outcome could not be confirmed within the original request budget.</summary>
[PublicAPI]
public sealed class CommitOutcomeUnknownException : Exception
{
    /// <summary>Stable detail shared with the server transport contract.</summary>
    internal const string StableDetail = "COMMIT_OUTCOME_UNKNOWN";

    /// <summary>Initializes a new instance of the <see cref="CommitOutcomeUnknownException" /> class.</summary>
    public CommitOutcomeUnknownException()
        : base(StableDetail)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CommitOutcomeUnknownException" /> class with a message.</summary>
    /// <param name="message">The exception message.</param>
    public CommitOutcomeUnknownException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CommitOutcomeUnknownException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The original transport exception.</param>
    public CommitOutcomeUnknownException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
