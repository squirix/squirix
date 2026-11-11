using System;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Admits every cache RPC before routing, fingerprinting, replay or joining an existing operation.</summary>
[Immutable]
internal sealed class CacheAdmissionInterceptor : Interceptor
{
    private readonly IBackpressureClientIdResolver _clients;
    private readonly IBackpressureGate _gate;

    internal CacheAdmissionInterceptor(IBackpressureGate gate, IBackpressureClientIdResolver clients)
    {
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(clients);
        _gate = gate;
        _clients = clients;
    }

    /// <inheritdoc />
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var (decision, lease) = await _gate.AcquireAsync(CacheOperationNames.CacheTransport, Operation(context.Method), _clients.Resolve(), context.CancellationToken)
                                         .ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown").ToRpcException();

        using (lease)
        using (InboundAdmissionScope.Enter(_gate))
            return await continuation(request, context).ConfigureAwait(false);
    }

    private static string Operation(string method) => method.AsSpan(method.AsSpan().LastIndexOf('/') + 1) switch
    {
        "GetEntry" or "GetExpiration" => CacheOperationNames.GetEntry,
        "GetValue" or "GetDistributed" => CacheOperationNames.Get,
        "SetEntry" or "SetDistributed" => CacheOperationNames.Set,
        "TryAddEntry" => CacheOperationNames.TryAdd,
        "GetOrAdd" => CacheOperationNames.GetOrAdd,
        "Remove" => CacheOperationNames.Remove,
        "RemoveExpiration" => CacheOperationNames.RemoveExpiration,
        "Touch" or "RefreshDistributed" => CacheOperationNames.Touch,
        "Update" => CacheOperationNames.Update,
        _ => throw new InvalidOperationException("Unsupported cache RPC admission operation."),
    };
}
