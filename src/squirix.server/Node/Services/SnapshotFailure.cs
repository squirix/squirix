using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>The most recent failed snapshot attempt, published as one immutable value.</summary>
/// <param name="ExceptionType">The simple name of the exception type that failed the attempt.</param>
/// <param name="Message">The exception message.</param>
/// <param name="ConsecutiveFailures">How many attempts in a row have failed, this one included.</param>
/// <param name="OccurredUtc">When the attempt failed.</param>
[Immutable]
internal sealed record SnapshotFailure(string ExceptionType, string Message, int ConsecutiveFailures, DateTimeOffset OccurredUtc);
