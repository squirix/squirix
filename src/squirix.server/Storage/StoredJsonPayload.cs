using System;
using System.Buffers;
using System.Text.Json;

namespace Squirix.Server.Storage;

/// <summary>
/// Owns raw UTF-8 JSON bytes for cache values stored with the <c>"j"</c> discriminator tag.
/// Immutable reference type — safe to share across threads without cloning.
/// Parsing is deferred until a consumer calls <see cref="WriteTo" /> or reads via discriminated entry conversion.
/// </summary>
internal sealed class StoredJsonPayload
{
    private readonly byte[] _utf8;

    public StoredJsonPayload(ReadOnlySpan<byte> utf8)
    {
        _utf8 = [.. utf8];
    }

    public ReadOnlyMemory<byte> Utf8Memory => _utf8;

    /// <summary>
    /// Creates a <see cref="StoredJsonPayload" /> by capturing the raw UTF-8 bytes of a <see cref="JsonElement" />.
    /// </summary>
    /// <param name="element">The JSON element to capture.</param>
    /// <returns>A payload owning a copy of the element UTF-8 bytes.</returns>
    public static StoredJsonPayload FromElement(JsonElement element)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            element.WriteTo(writer);
        }

        return new StoredJsonPayload(buffer.WrittenSpan);
    }

    public void WriteTo(Utf8JsonWriter writer)
    {
        using var doc = JsonDocument.Parse(_utf8);
        doc.RootElement.WriteTo(writer);
    }
}
