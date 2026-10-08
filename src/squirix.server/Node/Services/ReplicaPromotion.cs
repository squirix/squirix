using System.Runtime.InteropServices;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>One leadership the election started on this node, for the readiness service to verify the followers of.</summary>
/// <param name="Committer">The committer of the led group.</param>
/// <param name="Tenure">The token canceled when the leadership ends.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaPromotion(ReplicaGroupCommitter Committer, CancellationToken Tenure);
