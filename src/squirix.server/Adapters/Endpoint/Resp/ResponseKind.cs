namespace Squirix.Server.Adapters.Endpoint.Resp;

internal enum ResponseKind
{
    /// <summary>A version-specific null.</summary>
    Null = 0,

    /// <summary>A complete, encoded reply.</summary>
    Encoded = 1,

    /// <summary>A bulk byte sequence.</summary>
    Bulk = 2,

    /// <summary>A signed decimal integer.</summary>
    Integer = 3,
}
