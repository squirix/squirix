using System;
using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Grpc;

internal static class CallErrors
{
    internal static ServerCallException FromGrpc(RpcException failure)
    {
        var error = new BinaryCallError { Detail = failure.Status.Detail };
        foreach (var entry in failure.Trailers)
        {
            if (entry.Key.StartsWith("grpc-", StringComparison.OrdinalIgnoreCase))
                continue;

            var trailer = new BinaryCallError.Types.Trailer { Key = entry.Key };
            if (entry.IsBinary)
                trailer.Binary = ByteString.CopyFrom(entry.ValueBytes);
            else
                trailer.Text = entry.Value;
            error.Trailers.Add(trailer);
        }

        return new ServerCallException(Unsafe.BitCast<StatusCode, FrameStatus>(failure.StatusCode), error, failure.Status.DebugException);
    }

    internal static RpcException ToGrpc(ServerCallException failure)
    {
        var trailers = new Metadata();
        foreach (var entry in failure.Error.Trailers)
        {
            if (entry.ValueCase == BinaryCallError.Types.Trailer.ValueOneofCase.Binary)
                trailers.Add(entry.Key, entry.Binary.ToByteArray());
            else
                trailers.Add(entry.Key, entry.Text);
        }

        return new RpcException(new Status(Unsafe.BitCast<FrameStatus, StatusCode>(failure.StatusCode), failure.Message), trailers);
    }
}
