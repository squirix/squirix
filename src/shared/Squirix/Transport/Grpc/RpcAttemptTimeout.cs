using System;

namespace Squirix.Transport.Grpc;

/// <summary>The time one cache RPC attempt may take, shared by the SDK and the server so a wait on one side can be sized against the other.</summary>
/// <remarks>
/// The SDK applies it to an attempt as a cancellation token that the server never sees, and the server applies it to a forwarded call. A
/// node that refuses a call after waiting must therefore answer within this time, or the caller sees a timeout instead of the refusal.
/// </remarks>
internal static class RpcAttemptTimeout
{
    /// <summary>The number of transport-level attempts the SDK makes for one RPC.</summary>
    internal const int MaxAttempts = 3;

    /// <summary>Gets the per-attempt timeout.</summary>
    internal static TimeSpan PerAttempt { get; } = TimeSpan.FromSeconds(3);

    /// <summary>Gets the absolute deadline of one SDK operation across endpoint failover, stale-term reroute and transport retries.</summary>
    /// <remarks>
    /// It is <see cref="PerAttempt" /> multiplied by <see cref="MaxAttempts" /> plus two, so one fully hung endpoint consumes at most the
    /// retry budget of its own attempts and still leaves at least one full attempt for the next endpoint. A retry of the SDK reaches the
    /// server within this time of the first attempt, which sizes the window the server keeps operation outcomes for.
    /// </remarks>
    internal static TimeSpan OperationDeadline => PerAttempt * (MaxAttempts + 2);
}
