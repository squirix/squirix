namespace Squirix.Benchmarks.Payload;

/// <summary>Payload sizes used to compare serialization overhead across typical and near-limit entries.</summary>
public enum EntryPayloadProfile
{
    /// <summary>A 256-byte string payload.</summary>
    Small256B,

    /// <summary>A 64 KiB string payload.</summary>
    Medium64KiB,

    /// <summary>A 1 MiB string payload.</summary>
    Large1MiB,

    /// <summary>A string payload at the fixed entry size limit.</summary>
    NearLimitEntry,
}
