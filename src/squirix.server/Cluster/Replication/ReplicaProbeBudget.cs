using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Bounds a parallel follower probe by time and by the number of answers that end it.</summary>
/// <param name="Timeout">Per-probe budget.</param>
/// <param name="AnswersNeeded">
/// The answers from a follower log (accepted or mismatching) after which the probes still pending are given up;
/// <see cref="int.MaxValue" /> awaits every probe.
/// </param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaProbeBudget(TimeSpan Timeout, int AnswersNeeded);
