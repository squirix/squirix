namespace Squirix.Server.Core;

/// <summary>Identifies the representation of a stored server value.</summary>
internal enum StoredValueKind : long
{
    /// <summary>Null value.</summary>
    Null = 0,

    /// <summary>Boolean value.</summary>
    Boolean = 1,

    /// <summary>Integer value.</summary>
    Integer = 2,

    /// <summary>Double value.</summary>
    Double = 3,

    /// <summary>Text value.</summary>
    Text = 4,

    /// <summary>Binary value.</summary>
    Binary = 5,

    /// <summary>Json value.</summary>
    Json = 6,
}
