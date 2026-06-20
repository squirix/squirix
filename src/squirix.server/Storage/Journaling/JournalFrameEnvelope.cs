namespace Squirix.Server.Storage.Journaling;

/// <summary>Shared length-prefix and CRC32C footer layout for JsonFramed and Pipelined journal frames.</summary>
internal static class JournalFrameEnvelope
{
    internal const int HeaderSize = 4;

    internal const int FooterSize = 4;

    internal static int TotalLength(int bodyLength) => HeaderSize + bodyLength + FooterSize;
}
