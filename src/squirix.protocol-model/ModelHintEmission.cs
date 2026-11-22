using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>
/// Checker evidence about the successors one member named in one term: the first it named, and the first different one
/// after that.
/// </summary>
/// <param name="Sender">The member that named a successor.</param>
/// <param name="Term">The term it named the successor in.</param>
/// <param name="FirstSuccessor">The successor it named first.</param>
/// <param name="SecondSuccessor">The first different successor it named afterwards, or -1 when it named only one.</param>
/// <remarks>
/// <para>
/// One record per member and term, not one per emission. Keeping every emission would make the fault unbounded — a leader
/// that is not leaving can emit again after each delivery, so the history would grow without limit, the exploration would
/// distinguish ever more states for one behaviour, and it would never reach a fixed point. What the policy needs is
/// bounded: the first successor named, and whether a different one followed. The second is recorded once and the record
/// then stops changing, so the fault costs one extra state value per term rather than an unbounded list.
/// </para>
/// <para>
/// Two copies of one hint name one successor, so they leave the record unchanged; the recorded pair is the first
/// <em>different</em> successor, which is the event the policy is about.
/// </para>
/// </remarks>
[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly record struct ModelHintEmission(int Sender, int Term, int FirstSuccessor, int SecondSuccessor);
