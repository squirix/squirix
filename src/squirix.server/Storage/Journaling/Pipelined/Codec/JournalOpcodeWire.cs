using System.Globalization;
using System.IO;

namespace Squirix.Server.Storage.Journaling.Pipelined.Codec;

internal static class JournalOpcodeWire
{
    internal static JournalOpcode FromByte(byte value) => value switch
    {
        1 => JournalOpcode.Put,
        2 => JournalOpcode.Remove,
        3 => JournalOpcode.RemoveExpiration,
        4 => JournalOpcode.TouchExpiration,
        _ => throw new InvalidDataException($"unknown journal opcode {value.ToString(CultureInfo.InvariantCulture)}."),
    };

    internal static byte ToWireValue(JournalOpcode opcode) => opcode switch
    {
        JournalOpcode.Put => 1,
        JournalOpcode.Remove => 2,
        JournalOpcode.RemoveExpiration => 3,
        JournalOpcode.TouchExpiration => 4,
        _ => throw new InvalidDataException($"unknown journal opcode {opcode.ToString()}."),
    };
}
