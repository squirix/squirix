using BenchmarkDotNet.Attributes;

namespace Squirix.Benchmarks.Payload;

/// <summary>Measures client-side metadata resolution and serialization of a value.</summary>
[MemoryDiagnoser]
public class ClientSerializerMetadataBenchmarks
{
    private const string Value = "benchmark-value";

    /// <summary>Serializes a value declared as <see cref="object" />, resolved by its runtime type.</summary>
    /// <returns>Serialized byte length to prevent dead-code elimination.</returns>
    [Benchmark]
    public int SerializeByRuntimeType() => ClientSerializerMetadata.SerializeToUtf8Bytes(Value, typeof(object)).Length;

    /// <summary>Serializes a value whose declared type matches its runtime type.</summary>
    /// <returns>Serialized byte length to prevent dead-code elimination.</returns>
    [Benchmark]
    public int SerializeByDeclaredType() => ClientSerializerMetadata.SerializeToUtf8Bytes(Value, typeof(string)).Length;
}
