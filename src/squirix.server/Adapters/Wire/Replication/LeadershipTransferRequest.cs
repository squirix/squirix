using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Adapters.Wire.Replication;

internal sealed class LeadershipTransferRequest : WireMessage
{
    internal static WireParser<LeadershipTransferRequest> Parser { get; } = new(Read);

    internal required EnvelopeHeader Header { get; init; }

    internal required string Target { get; init; }

    internal override void Encode(ref WireWriter writer)
    {
        Header.Encode(ref writer);
        writer.Text(Target);
    }

    private static LeadershipTransferRequest Read(ref WireReader reader) => new() { Header = EnvelopeHeader.Read(ref reader), Target = reader.Text() };
}
