using System.Collections.Generic;

namespace Squirix.ProtocolModel;

/// <summary>Mutable working set shared across a single message-delivery transition.</summary>
internal sealed class TransitionScratch
{
    internal TransitionScratch(NodeState[] nodes, List<InFlightMessage> messages, int[] match, int nextMessageId)
    {
        Nodes = nodes;
        Messages = messages;
        Match = match;
        NextMessageId = nextMessageId;
    }

    internal int[] Match { get; }

    internal List<InFlightMessage> Messages { get; }

    internal int NextMessageId { get; set; }

    internal NodeState[] Nodes { get; }
}
