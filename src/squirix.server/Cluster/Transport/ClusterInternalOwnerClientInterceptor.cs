using System;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Marks outbound cluster owner-routing gRPC calls for trusted inter-node authentication.
/// </summary>
internal sealed class ClusterInternalOwnerClientInterceptor : Interceptor
{
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var callOptions = AttachInternalOwnerHeader(context.Options);
        var updatedContext = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, callOptions);
        return base.AsyncUnaryCall(request, updatedContext, continuation);
    }

    private static CallOptions AttachInternalOwnerHeader(CallOptions options)
    {
        var metadata = options.Headers ?? [];
        Upsert(metadata, RemoteInvocationContract.InternalOwnerRpcHeaderName, RemoteInvocationContract.InternalOwnerRpcHeaderValue);
        return new CallOptions(metadata, options.Deadline, options.CancellationToken, options.WriteOptions, options.PropagationToken, options.Credentials);
    }

    private static void Upsert(Metadata metadata, string key, string value)
    {
        for (var i = 0; i < metadata.Count; i++)
        {
            if (!string.Equals(metadata[i].Key, key, StringComparison.Ordinal))
                continue;

            metadata.RemoveAt(i);
            break;
        }

        metadata.Add(key, value);
    }
}
