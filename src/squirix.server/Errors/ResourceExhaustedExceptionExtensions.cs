using Grpc.Core;

namespace Squirix.Server.Errors;

internal static class ResourceExhaustedExceptionExtensions
{
    /// <summary>
    /// Maps <see cref="ResourceExhaustedException" /> to transport-specific error representations.
    /// </summary>
    extension(ResourceExhaustedException exception)
    {
        /// <summary>
        /// Maps memory-pressure rejection to gRPC <see cref="StatusCode.ResourceExhausted" /> with bounded detail.
        /// </summary>
        /// <returns>A <see cref="RpcException" /> for the failure.</returns>
        public RpcException ToRpcException()
        {
            _ = exception;
            return CacheOperationContract.MemoryPressure().ToRpcException();
        }
    }
}
