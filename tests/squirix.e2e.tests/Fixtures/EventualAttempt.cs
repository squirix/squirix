using System;
using Squirix.Attributes;

namespace Squirix.E2ETests.Fixtures;

/// <summary>One attempt of an <see cref="Eventually" /> retry: when it started, how long it ran and how it ended.</summary>
/// <param name="Started">When the attempt started, from the start of the retry.</param>
/// <param name="Elapsed">How long the attempt ran.</param>
/// <param name="Outcome">"ok", or the type and status of the failure.</param>
[Immutable]
internal readonly record struct EventualAttempt(TimeSpan Started, TimeSpan Elapsed, string Outcome);
