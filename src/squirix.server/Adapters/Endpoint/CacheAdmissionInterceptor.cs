using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Admits every cache RPC before routing, fingerprinting, replay or joining an existing operation.</summary>
[Immutable]
internal sealed class CacheAdmissionInterceptor : Interceptor
{
    private readonly CacheRequestAdmission _admission;

    public CacheAdmissionInterceptor(CacheRequestAdmission admission)
    {
        _admission = admission;
    }

    /// <inheritdoc />
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var (decision, lease) = await _admission.AcquireAsync(context.Method, context.CancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown").ToRpcException();

        using (lease)
        using (_admission.Enter())
            return await continuation(request, context).ConfigureAwait(false);
    }
}
