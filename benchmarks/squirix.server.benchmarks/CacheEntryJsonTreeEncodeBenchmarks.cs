using System.Globalization;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of measuring and writing a JSON object whose values and property names are strings, paid once per journaled or snapshotted entry.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class CacheEntryJsonTreeEncodeBenchmarks
{
    private const int PropertyCount = 20;
    private const int ArrayItemCount = 8;

    private byte[] _destination = [];
    private JsonElement _element;
    private JsonElement _escapedElement;

    /// <summary>Parses the plain and the escaped object and sizes the destination.</summary>
    [GlobalSetup]
    public void Setup()
    {
        using var document = JsonDocument.Parse(BuildJson(false));
        _element = document.RootElement.Clone();
        using var escapedDocument = JsonDocument.Parse(BuildJson(true));
        _escapedElement = escapedDocument.RootElement.Clone();
        _destination = new byte[CacheEntryCodec.ComputeValueLength(_element) + CacheEntryCodec.ComputeValueLength(_escapedElement)];
    }

    /// <summary>Measures and writes an object with unescaped string values and property names.</summary>
    /// <returns>The number of bytes written.</returns>
    [Benchmark(Baseline = true)]
    public int EncodePlainStrings()
    {
        var length = CacheEntryCodec.ComputeValueLength(_element);
        CacheEntryCodec.WriteValue(_element, _destination);
        return length;
    }

    /// <summary>Measures and writes an object whose strings contain escape sequences.</summary>
    /// <returns>The number of bytes written.</returns>
    [Benchmark]
    public int EncodeEscapedStrings()
    {
        var length = CacheEntryCodec.ComputeValueLength(_escapedElement);
        CacheEntryCodec.WriteValue(_escapedElement, _destination);
        return length;
    }

    private static string BuildJson(bool escaped)
    {
        var builder = new StringBuilder("{");
        for (var i = 0; i < PropertyCount; i++)
        {
            var suffix = escaped ? @"\n\u00e9" : string.Empty;
            builder.Append(CultureInfo.InvariantCulture, $"\"property{i}{suffix}\":\"value number {i} with some text{suffix}\",");
        }

        builder.Append("\"items\":[");
        for (var i = 0; i < ArrayItemCount; i++)
        {
            if (i > 0)
                builder.Append(',');

            builder.Append(CultureInfo.InvariantCulture, $"\"item {i} text\"");
        }

        return builder.Append("]}").ToString();
    }
}
