using System;
using System.Text;
using Squirix.Server.Core;

namespace Squirix.Server.TestKit;

internal static class StoredValueProbe
{
    internal static string? Text(StoredValue? value) => value switch
    {
        null => null,
        { Kind: StoredValueKind.Text } text => Encoding.UTF8.GetString(text.Bytes),
        _ => throw new InvalidOperationException("Expected a text cache value."),
    };

    internal static byte[] Binary(StoredValue? value) => value is { Kind: StoredValueKind.Binary } binary
        ? binary.Bytes
        : throw new InvalidOperationException("Expected a binary cache value.");
}
