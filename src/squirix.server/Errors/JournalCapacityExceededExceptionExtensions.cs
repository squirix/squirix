using Grpc.Core;

namespace Squirix.Server.Errors;

internal static class JournalCapacityExceededExceptionExtensions
{
    /// <summary>
    /// Maps <see cref="JournalCapacityExceededException" /> to transport-specific error representations.
    /// </summary>
    extension(JournalCapacityExceededException exception)
    {
        /// <summary>
        /// Maps journal disk quota rejection to gRPC <see cref="StatusCode.ResourceExhausted" /> with bounded detail.
        /// </summary>
        /// <returns>A <see cref="RpcException" /> for the failure.</returns>
        internal RpcException ToRpcException()
        {
            _ = exception;
            return ServerOpContract.JournalDiskQuota().ToRpcException();
        }
    }
}
