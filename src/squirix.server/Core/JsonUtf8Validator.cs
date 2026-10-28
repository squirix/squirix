using System;
using System.Text.Json;
using System.Text.Unicode;

namespace Squirix.Server.Core;

/// <summary>Checks that bytes are exactly one well-formed JSON value in UTF-8.</summary>
internal static class JsonUtf8Validator
{
    /// <summary>The deepest nesting a value may have; the default depth limit of the JSON reader.</summary>
    internal const int MaxDepth = 64;

    private const byte BomFirstByte = 0xEF;
    private const byte BomSecondByte = 0xBB;
    private const byte BomThirdByte = 0xBF;

    private static readonly JsonReaderOptions ReaderOptions = new()
    {
        MaxDepth = MaxDepth,
        CommentHandling = JsonCommentHandling.Disallow,
        AllowTrailingCommas = false,
    };

    private static ReadOnlySpan<byte> ByteOrderMark => [BomFirstByte, BomSecondByte, BomThirdByte];

    /// <summary>Tells whether the bytes are one JSON value: no byte order mark, valid UTF-8, no comments or trailing commas, nesting within <see cref="MaxDepth" />, nothing after the value but white space.</summary>
    /// <param name="utf8">The bytes to check.</param>
    /// <returns><see langword="true" /> when the bytes are one valid JSON value.</returns>
    internal static bool IsValid(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty || utf8.StartsWith(ByteOrderMark) || !Utf8.IsValid(utf8))
            return false;

        try
        {
            var reader = new Utf8JsonReader(utf8, ReaderOptions);
            if (!reader.Read())
                return false;

            reader.Skip();
            return !reader.Read();
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
