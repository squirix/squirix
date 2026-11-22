using Squirix.Attributes;
using Squirix.Transport;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Internal.Cluster.Transport;

[Immutable]
internal readonly record struct BinaryClientReply(WireMessage? Response, SquirixTransportException? Failure);
