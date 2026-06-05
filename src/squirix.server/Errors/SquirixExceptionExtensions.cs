using Grpc.Core;

namespace Squirix.Server.Errors;

internal static class SquirixExceptionExtensions
{
    extension(SquirixException exception)
    {
        public RpcException ToRpcException() => new(new Status(SquirixErrorMapper.ToGrpcStatusCode(exception.Code), exception.Detail ?? exception.Error));
    }
}
