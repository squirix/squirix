using System;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Marks outbound cluster owner-routing gRPC calls for trusted internode authentication and carries the ring fingerprint of this node.</summary>
internal sealed class InternalOwnerClientInterceptor : Interceptor
{
    private static readonly Metadata.Entry InternalOwnerEntry = new(RemoteInvocationContract.InternalOwnerRpcHeaderName, RemoteInvocationContract.InternalOwnerRpcHeaderValue);

    private readonly Metadata.Entry _ringFingerprintEntry;

    internal InternalOwnerClientInterceptor(RingFingerprint ringFingerprint)
    {
        ArgumentNullException.ThrowIfNull(ringFingerprint);
        _ringFingerprintEntry = new Metadata.Entry(RemoteInvocationContract.RingFingerprintHeaderName, ringFingerprint.Value);
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var callOptions = AttachInternalOwnerHeaders(context.Options);
        var updatedContext = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, callOptions);
        return base.AsyncUnaryCall(request, updatedContext, continuation);
    }

    private static void Upsert(Metadata metadata, Metadata.Entry entry)
    {
        for (var i = 0; i < metadata.Count; i++)
        {
            if (!string.Equals(metadata[i].Key, entry.Key, StringComparison.Ordinal))
                continue;

            metadata.RemoveAt(i);
            break;
        }

        metadata.Add(entry);
    }

    private CallOptions AttachInternalOwnerHeaders(CallOptions options)
    {
        // Clone caller headers into a fresh bag; never mutate options.Headers in place,
        // so a Metadata instance shared across calls cannot bleed internal headers or race.
        var metadata = new Metadata();
        var callerHeaders = options.Headers;
        if (callerHeaders != null)
            GrpcMetadata.CopyInto(metadata, callerHeaders);

        Upsert(metadata, InternalOwnerEntry);
        Upsert(metadata, _ringFingerprintEntry);
        return new CallOptions(metadata, options.Deadline, options.CancellationToken, options.WriteOptions, options.PropagationToken, options.Credentials);
    }
}
