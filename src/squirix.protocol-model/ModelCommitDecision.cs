using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>Checker evidence of the earliest term in which a log entry was committed.</summary>
/// <param name="Index">The committed index.</param>
/// <param name="EntryTerm">The term of the stored entry.</param>
/// <param name="CommitTerm">The term in which a commit decision was observed.</param>
[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct ModelCommitDecision(int Index, int EntryTerm, int CommitTerm);
