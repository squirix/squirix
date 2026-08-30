using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Bundle of server call-policy instrumentation metrics resolved from DI.</summary>
/// <param name="CallPolicyMetrics">Per-host call-policy counters.</param>
/// <param name="RpcTimeoutMetrics">Per-host RPC timeout counters.</param>
[Immutable]
internal sealed record ServerCallPolicyInstrumentation(ServerCallPolicyMetrics CallPolicyMetrics, ServerRpcTimeoutMetrics RpcTimeoutMetrics);
