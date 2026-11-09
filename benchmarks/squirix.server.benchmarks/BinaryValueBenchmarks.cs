using System;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using Google.Protobuf;
using Squirix.Server.Serialization.Grpc;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Benchmarks;

/// <summary>Compares owned binary mapping with JSON/Base64 mapping of identical byte arrays, excluding networking.</summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class BinaryValueBenchmarks
{
    private byte[] _payload = [];
    private CacheValue _binary = new();
    private CacheValue _json = new();

    /// <summary>Gets or sets the unencoded payload size.</summary>
    [Params(1024, 65536)]
    public int PayloadBytes { get; set; }

    /// <summary>Creates repeatable arbitrary bytes and pre-encodes the decoder inputs.</summary>
    /// <exception cref="InvalidOperationException">A comparison path changes the payload.</exception>
    [GlobalSetup]
    public void Setup()
    {
        _payload = new byte[PayloadBytes];
        for (var i = 0; i < _payload.Length; i++)
            _payload[i] = byte.CreateTruncating(i);
        _binary = CacheValueWire.FromBinary(_payload);
        _json = CreateJson();
        if (!DecodeBinary().AsSpan().SequenceEqual(_payload) || !DecodeJson().AsSpan().SequenceEqual(_payload))
            throw new InvalidOperationException("The benchmark mappings must preserve the payload.");
    }

    /// <summary>Encodes a JSON string containing Base64, retaining the serialized bytes as the wire message owns them.</summary>
    /// <returns>The owned JSON wire value.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Encode")]
    public object EncodeJson() => CreateJson();

    /// <summary>Copies raw bytes into the binary wire discriminator.</summary>
    /// <returns>The owned binary wire value.</returns>
    [Benchmark]
    [BenchmarkCategory("Encode")]
    public object EncodeBinary() => CacheValueWire.FromBinary(_payload);

    /// <summary>Decodes the JSON/Base64 string into an owned array.</summary>
    /// <returns>The decoded bytes.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Decode")]
    public byte[] DecodeJson() => JsonSerializer.Deserialize(_json.JsonUtf8.Span, BinaryBenchmarkJson.Default.ByteArray)!;

    /// <summary>Decodes the binary discriminator into an owned array.</summary>
    /// <returns>The decoded bytes.</returns>
    [Benchmark]
    [BenchmarkCategory("Decode")]
    public byte[] DecodeBinary() => ServerProtoEx.MapCacheValue<byte[]>(_binary)!;

    private CacheValue CreateJson() => new() { JsonUtf8 = UnsafeByteOperations.UnsafeWrap(JsonSerializer.SerializeToUtf8Bytes(_payload, BinaryBenchmarkJson.Default.ByteArray)) };
}
