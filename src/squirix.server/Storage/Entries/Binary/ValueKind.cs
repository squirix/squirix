namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Tagged cache-value kinds in binary snapshot/journal payloads.</summary>
internal enum ValueKind : byte
{
    /// <summary>Null value.</summary>
    Null = 0,

    /// <summary>Boolean value.</summary>
    Bool = 1,

    /// <summary>UTF-8 string value.</summary>
    String = 2,

    /// <summary>Raw byte array value.</summary>
    Bytes = 3,

    /// <summary>64-bit integer value.</summary>
    Int64 = 4,

    /// <summary>IEEE double value.</summary>
    Double = 5,

    /// <summary>Decimal serialized as invariant text.</summary>
    Decimal = 6,

    /// <summary>Raw UTF-8 JSON blob.</summary>
    JsonBlob = 7,
}
