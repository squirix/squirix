using System;
using System.IO;
using Squirix.Server.Errors;

namespace Squirix.Server.Node.Services;

/// <summary>The faults of applying committed entries to local memory and the node journal that the apply, readiness and promotion paths retry instead of failing.</summary>
internal static class ReplicaRetryableFault
{
    /// <summary>Tells whether a fault of applying committed entries leaves the work pending for the next pass instead of faulting its loop.</summary>
    /// <param name="exception">The fault.</param>
    /// <returns><see langword="true" /> for storage and journal faults, and for a full journal or memory that refused the write.</returns>
    internal static bool Is(Exception exception) =>
        exception is IOException or InvalidOperationException or JournalCapacityExceededException or ResourceExhaustedException;
}
