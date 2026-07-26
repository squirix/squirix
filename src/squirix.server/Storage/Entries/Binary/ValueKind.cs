namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Tagged cache-value kinds in binary snapshot/journal payloads.</summary>
internal static class ValueKind
{
    /// <summary>JSON array encoded as a recursive binary tree.</summary>
    internal const byte Array = 8;

    /// <summary>Boolean value.</summary>
    internal const byte Bool = 1;

    /// <summary>Raw byte array value.</summary>
    internal const byte Bytes = 3;

    /// <summary>Decimal serialized as invariant text.</summary>
    internal const byte Decimal = 6;

    /// <summary>IEEE double value.</summary>
    internal const byte Double = 5;

    /// <summary>64-bit integer value.</summary>
    internal const byte Int64 = 4;

    /// <summary>Null value.</summary>
    internal const byte Null = 0;

    /// <summary>JSON object encoded as a recursive binary tree.</summary>
    internal const byte Object = 7;

    /// <summary>UTF-8 string value.</summary>
    internal const byte String = 2;
}
