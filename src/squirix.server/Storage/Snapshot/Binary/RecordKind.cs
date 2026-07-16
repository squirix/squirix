namespace Squirix.Server.Storage.Snapshot.Binary;

internal enum RecordKind
{
    /// <summary>Cache entry record.</summary>
    Entry = 1,

    /// <summary>Idempotency record.</summary>
    Idempotency = 2,
}
