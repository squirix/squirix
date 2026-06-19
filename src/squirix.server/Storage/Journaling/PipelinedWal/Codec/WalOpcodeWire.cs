using System.Globalization;
using System.IO;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Codec;

internal static class WalOpcodeWire
{
    internal static WalOpcode FromByte(byte value) => value switch
    {
        1 => WalOpcode.Put,
        2 => WalOpcode.Remove,
        3 => WalOpcode.RemoveExpiration,
        4 => WalOpcode.TouchExpiration,
        _ => throw new InvalidDataException($"unknown WAL opcode {value.ToString(CultureInfo.InvariantCulture)}."),
    };

    internal static byte ToWireValue(WalOpcode opcode) => opcode switch
    {
        WalOpcode.Put => 1,
        WalOpcode.Remove => 2,
        WalOpcode.RemoveExpiration => 3,
        WalOpcode.TouchExpiration => 4,
        _ => throw new InvalidDataException($"unknown WAL opcode {opcode.ToString()}."),
    };
}
