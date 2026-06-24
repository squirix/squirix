namespace Squirix.Benchmarks.Payload;

/// <summary>Payload sizes used to compare serialization overhead across typical and near-limit entries.</summary>
public enum EntryPayloadProfile
{
    /// <summary>A 256-byte string payload.</summary>
    Small256B = 0,

    /// <summary>A 64 KiB string payload.</summary>
    Medium64KiB = 1,

    /// <summary>A 1 MiB string payload.</summary>
    Large1MiB = 2,

    /// <summary>A string payload at the fixed entry size limit.</summary>
    NearLimitEntry = 3,
}
