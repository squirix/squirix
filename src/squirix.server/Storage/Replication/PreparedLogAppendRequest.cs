using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>An append over immutable, non-pooled payloads with explicit ownership.</summary>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly struct PreparedLogAppendRequest
{
    private PreparedLogAppendRequest(in FollowerLogAppendRequest request)
    {
        Request = request;
    }

    /// <summary>Gets the append view over owned immutable payloads.</summary>
    internal FollowerLogAppendRequest Request { get; }

    /// <summary>Takes ownership of a request whose entries and payloads are already validated and immutable.</summary>
    /// <param name="request">The append over stable, non-pooled payload memory.</param>
    /// <returns>The request with an explicit payload ownership contract.</returns>
    /// <remarks>The caller must never change the entry array or payload bytes afterwards, or return them to a pool.</remarks>
    internal static PreparedLogAppendRequest TakeOwnership(in FollowerLogAppendRequest request) => new(in request);
}
