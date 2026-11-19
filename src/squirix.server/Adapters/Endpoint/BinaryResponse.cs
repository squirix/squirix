using Google.Protobuf;
using Squirix.Server.Errors;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Adapters.Endpoint;

internal readonly record struct BinaryResponse(FrameStatus Status, IMessage? Payload = null)
{
    internal static BinaryResponse FromError(SquirixException error) =>
        new(SquirixErrorMapper.ToFrameStatus(error.Code), new BinaryCallError { Detail = error.Detail ?? error.Error });
}
