using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>What a passed <see cref="GroupLogAudit" /> compared.</summary>
/// <param name="SharedFrom">The first index of the committed range every audited node retains; above <paramref name="SharedTo" /> when the range is empty.</param>
/// <param name="SharedTo">The last index of the committed range every audited node retains: the commit index of the group.</param>
/// <param name="ClientEntries">The number of client entries in the shared range.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct GroupLogAuditReport(ulong SharedFrom, ulong SharedTo, int ClientEntries);
