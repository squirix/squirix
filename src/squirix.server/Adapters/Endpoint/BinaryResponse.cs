using Squirix.Server.Errors;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Mappers;

namespace Squirix.Server.Adapters.Endpoint;

internal readonly record struct BinaryResponse(FrameStatus Status, WireMessage? Payload = null)
{
    internal static BinaryResponse FromError(SquirixException error)
    {
        var payload = new BinaryCallError { Detail = error.Detail ?? error.Error };
        payload.Trailers.Add(new BinaryCallError.Types.Trailer { Key = StaleOwnerMarkers.ErrorCodeMetadataKey, Text = SquirixErrorMapper.ToPublicCode(error.Code) });
        return new BinaryResponse(SquirixErrorMapper.ToFrameStatus(error.Code), payload);
    }
}
