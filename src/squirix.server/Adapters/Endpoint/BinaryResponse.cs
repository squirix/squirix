using Google.Protobuf;
using Grpc.Core;

namespace Squirix.Server.Adapters.Endpoint;

internal readonly record struct BinaryResponse(StatusCode Status, IMessage? Payload = null);
