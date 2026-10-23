using System;

namespace Squirix.Transport.Grpc;

/// <summary>The time one cache RPC attempt may take, shared by the SDK and the server so a wait on one side can be sized against the other.</summary>
/// <remarks>
/// The SDK applies it to an attempt as a cancellation token that the server never sees, and the server applies it to a forwarded call. A
/// node that refuses a call after waiting must therefore answer within this time, or the caller sees a timeout instead of the refusal.
/// </remarks>
internal static class RpcAttemptTimeout
{
    /// <summary>Gets the per-attempt timeout.</summary>
    internal static TimeSpan PerAttempt { get; } = TimeSpan.FromSeconds(3);
}
