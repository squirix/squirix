namespace Squirix.Server.Storage.Journaling.Json;

internal sealed class RecordEnvelope
{
    public PutOp? Put { get; set; }

    public RemoveOp? Remove { get; set; }

    public RemoveExpirationOp? RemoveExpiration { get; set; }

    public ulong Seq { get; init; }

    public TouchExpirationOp? TouchExpiration { get; set; }

    public long UnixMs { get; init; }
}
