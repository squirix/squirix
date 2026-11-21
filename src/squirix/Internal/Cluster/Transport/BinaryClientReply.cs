using Google.Protobuf;
using Squirix.Attributes;
using Squirix.Transport;

namespace Squirix.Internal.Cluster.Transport;

[Immutable]
internal readonly record struct BinaryClientReply(IMessage? Response, SquirixTransportException? Failure);
