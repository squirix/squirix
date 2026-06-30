using System;
using System.Buffers.Binary;
using System.Text;
using System.Text.Json.Serialization.Metadata;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Pre-encoded UTF-8 property names for fast object decode lookup.</summary>
internal sealed class ObjectPropertyIndex
{
    private readonly EncodedPropertyEntry[] _entries;

    private ObjectPropertyIndex(EncodedPropertyEntry[] entries)
    {
        _entries = entries;
    }

    internal ReadOnlySpan<EncodedPropertyEntry> Entries => _entries;

    internal static ObjectPropertyIndex Create(JsonTypeInfo typeInfo)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        if (typeInfo.Kind is not JsonTypeInfoKind.Object)
            throw new InvalidOperationException("Property index requires object metadata.");

        var properties = typeInfo.Properties;
        if (properties.Count is 0)
            return new ObjectPropertyIndex([]);

        var entries = new EncodedPropertyEntry[properties.Count];
        Span<byte> scratch = stackalloc byte[128];
        for (var i = 0; i < properties.Count; i++)
        {
            var property = properties[i];
            var byteCount = Encoding.UTF8.GetByteCount(property.Name);
#pragma warning disable ZA0302
            var ownedName = new byte[byteCount];
#pragma warning restore ZA0302
            if (byteCount <= 128)
            {
                _ = Encoding.UTF8.GetBytes(property.Name, scratch[..byteCount]);
                scratch[..byteCount].CopyTo(ownedName);
            }
            else
            {
                _ = Encoding.UTF8.GetBytes(property.Name, ownedName);
            }

            entries[i] = new EncodedPropertyEntry(ownedName, property);
        }

        return new ObjectPropertyIndex(entries);
    }

    internal JsonPropertyInfo? Find(ReadOnlySpan<byte> utf8Name)
    {
        foreach (var entry in _entries)
        {
            if (utf8Name.SequenceEqual(entry.NameUtf8))
                return entry.Property;
        }

        return null;
    }

    internal readonly struct EncodedPropertyEntry
    {
        internal EncodedPropertyEntry(byte[] nameUtf8, JsonPropertyInfo property)
        {
            NameUtf8 = nameUtf8;
            Property = property;
        }

        internal byte[] NameUtf8 { get; }

        internal int PrefixedNameLength => 2 + NameUtf8.Length;

        internal JsonPropertyInfo Property { get; }

        internal int WritePrefixedName(Span<byte> destination)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(destination, ushort.CreateTruncating(NameUtf8.Length));
            NameUtf8.CopyTo(destination[2..]);
            return 2 + NameUtf8.Length;
        }
    }
}
