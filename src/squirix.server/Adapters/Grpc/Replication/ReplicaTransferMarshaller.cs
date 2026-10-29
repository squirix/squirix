using System.Buffers;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Utils;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Holds a received transfer buffer through HTTP completion, including failures before dispatch.</summary>
internal sealed class ReplicaTransferMarshaller
{
    private readonly IHttpContextAccessor _contexts;
    private readonly ArrayPool<byte> _pool;

    internal ReplicaTransferMarshaller(IHttpContextAccessor contexts, ArrayPool<byte> pool)
    {
        _contexts = contexts;
        _pool = pool;
    }

    internal TransferReplicaStateRequest Deserialize(DeserializationContext context)
    {
        var http = ThrowHelper.Required(_contexts.HttpContext, "A transfer requires an active HTTP request.");
        ReplicaTransferBuffer? buffer = ReplicaTransferWire.Parse(context.PayloadAsReadOnlySequence(), _pool);
        try
        {
            http.Response.RegisterForDispose(buffer);
            var request = buffer.Request;
            buffer = null;
            return request;
        }
        finally
        {
            buffer?.Dispose();
        }
    }
}
