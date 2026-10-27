using System.Runtime.InteropServices;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>One committed entry of a replica group log, as <see cref="GroupLogAudit" /> compares it.</summary>
/// <param name="Index">The log index.</param>
/// <param name="Term">The term the entry was created in.</param>
/// <param name="OperationScope">The operation scope of the record.</param>
/// <param name="OperationId">The client operation identifier of the record.</param>
/// <param name="MutationKind">The mutation kind of the record.</param>
/// <param name="PayloadHash">A hash of the canonical entry bytes.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct AuditedLogEntry(ulong Index, ulong Term, string OperationScope, string OperationId, string MutationKind, ulong PayloadHash)
{
    /// <summary>Gets a value indicating whether the entry carries a client operation: neither a leader no-op nor an expiry tombstone.</summary>
    internal bool IsClientEntry => MutationKind is not (ReplicaMutationKinds.LeaderNoop or ReplicaMutationKinds.Expire);
}
