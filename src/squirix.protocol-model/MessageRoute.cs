using System.Runtime.InteropServices;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
internal readonly record struct MessageRoute(MsgKind Kind, int From, int To, int Term);
