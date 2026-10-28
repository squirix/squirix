using System.Globalization;
using System.Text;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of measuring, writing and reading a structured value kept as JSON bytes, paid once per journaled, snapshotted or replicated entry.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class CacheEntryJsonValueBenchmarks
{
    private const int ArrayItemCount = 8;

    private byte[] _destination = [];
    private byte[] _encoded = [];
    private NodeCacheEntry<object?> _entry = new() { Value = null };

    /// <summary>Gets or sets the number of properties of the object.</summary>
    [Params(4, 64)]
    public int PropertyCount { get; set; }

    /// <summary>Builds the value, the entry around it and its encoded form.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var bytes = Encoding.UTF8.GetBytes(BuildJson(PropertyCount));
        _entry = new NodeCacheEntry<object?> { Value = JsonUtf8Value.TryCreateCopy(bytes, out var value) ? value : null };
        _destination = new byte[CacheEntryCodec.ComputeEncodedLength(_entry)];
        CacheEntryCodec.Write(_entry, _destination);
        _encoded = _destination;
    }

    /// <summary>Measures and writes the entry.</summary>
    /// <returns>The number of bytes written.</returns>
    [Benchmark(Baseline = true)]
    public int Encode()
    {
        var length = CacheEntryCodec.ComputeEncodedLength(_entry);
        return length + CacheEntryCodec.WriteMeasured(_entry, _destination);
    }

    /// <summary>Validates and copies the JSON bytes of the entry out of the encoded form.</summary>
    /// <returns>The number of bytes read.</returns>
    [Benchmark]
    public int Decode() => CacheEntryCodec.TryRead<object?>(_encoded, out _, out var bytesRead) ? bytesRead : 0;

    private static string BuildJson(int propertyCount)
    {
        var builder = new StringBuilder("{");
        for (var i = 0; i < propertyCount; i++)
            builder.Append(CultureInfo.InvariantCulture, $"\"property{i}\":\"value number {i} with some text\",");

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
