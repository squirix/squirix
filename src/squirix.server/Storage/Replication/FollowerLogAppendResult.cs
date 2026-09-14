using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of a replica log append attempt.</summary>
/// <param name="Success">Determines whether the appending was accepted.</param>
/// <param name="RefusalCode">Stable refusal marker when the appending was not accepted; otherwise empty.</param>
/// <param name="CurrentTerm">The durable term after processing the request.</param>
/// <param name="LastLogIndex">The durable last log index after processing the request.</param>
[Immutable]
internal readonly record struct FollowerLogAppendResult(bool Success, string RefusalCode, ulong CurrentTerm, ulong LastLogIndex);
