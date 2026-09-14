using System;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Marks outbound cluster owner-routing gRPC calls for trusted internode authentication.</summary>
internal sealed class InternalOwnerClientInterceptor : Interceptor
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
        // Clone caller headers into a fresh bag; never mutate options.Headers in place,
        // so a Metadata instance shared across calls cannot bleed internal headers or race.
        var metadata = new Metadata();
        var callerHeaders = options.Headers;
        if (callerHeaders != null)
            GrpcMetadata.CopyInto(metadata, callerHeaders);

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
