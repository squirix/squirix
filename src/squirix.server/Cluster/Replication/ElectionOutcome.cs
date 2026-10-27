using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The result of one step of the election driver of a group.</summary>
/// <param name="Event">What the step did.</param>
/// <param name="Term">The term the step ended in.</param>
/// <param name="Denial">Why the node may not campaign, for <see cref="ElectionEvent.Denied" />; otherwise <see cref="FailoverDenial.None" />.</param>
/// <param name="Waited">
/// How long the leader waited for authority, for a <see cref="ElectionEvent.SteppedDown" /> that ended a leadership which got no authority
/// within its deadline, how long its applied index stood still, for an <see cref="ElectionEvent.ApplyTimedOut" />, or how long its appends
/// failed, for an <see cref="ElectionEvent.AppendTimedOut" />; otherwise zero.
/// </param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ElectionOutcome(ElectionEvent Event, ulong Term, FailoverDenial Denial = FailoverDenial.None, TimeSpan Waited = default);
