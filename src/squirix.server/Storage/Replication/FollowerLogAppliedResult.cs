namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of a replica applied-index advance attempt.</summary>
/// <param name="Success">Determines whether the applied index was advanced.</param>
/// <param name="RefusalCode">Stable refusal marker when the applied index was not advanced; otherwise empty.</param>
/// <param name="AppliedIndex">The durable applied index after processing the request.</param>
internal readonly record struct FollowerLogAppliedResult(bool Success, string RefusalCode, ulong AppliedIndex);
