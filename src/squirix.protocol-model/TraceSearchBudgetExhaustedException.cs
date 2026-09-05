using System;
using System.Globalization;
using JetBrains.Annotations;

namespace Squirix.ProtocolModel;

/// <summary>Thrown when a trace search hits its state budget before the queue drains.</summary>
public sealed class TraceSearchBudgetExhaustedException : InvalidOperationException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TraceSearchBudgetExhaustedException" /> class.
    /// </summary>
    [UsedImplicitly]
    public TraceSearchBudgetExhaustedException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TraceSearchBudgetExhaustedException" /> class with the budget details.
    /// </summary>
    /// <param name="maxStates">The configured state budget that was exhausted.</param>
    /// <param name="visitedStates">The number of states visited before the budget was hit.</param>
    public TraceSearchBudgetExhaustedException(int maxStates, int visitedStates)
        : base(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Trace search hit the state budget before the queue drained (visited {visitedStates} of {maxStates} states). The trace was neither accepted nor rejected; increase MaxStates and retry."))
    {
        MaxStates = maxStates;
        VisitedStates = visitedStates;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TraceSearchBudgetExhaustedException" /> class with a message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    [UsedImplicitly]
    public TraceSearchBudgetExhaustedException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TraceSearchBudgetExhaustedException" /> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    [UsedImplicitly]
    public TraceSearchBudgetExhaustedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Gets the configured state budget that was exhausted.</summary>
    public int MaxStates { get; }

    /// <summary>Gets the number of states visited before the budget was hit.</summary>
    public int VisitedStates { get; }
}
