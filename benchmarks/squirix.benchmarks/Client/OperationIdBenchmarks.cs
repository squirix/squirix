using System;
using System.Security.Cryptography;
using System.Threading;
using BenchmarkDotNet.Attributes;

namespace Squirix.Benchmarks.Client;

/// <summary>Compares operation identifier generation strategies.</summary>
[MemoryDiagnoser]
public class OperationIdBenchmarks
{
    private const int BufferSize = 4096;
    private const int IdBytes = 16;
    private const int CallsPerThread = 20_000;

    [ThreadStatic]
    private static byte[]? _buffer;

    [ThreadStatic]
    private static int _position;

    /// <summary>Gets or sets the number of concurrent threads for the multi-threaded benchmarks.</summary>
    [Params(8, 32)]
    public int Threads { get; set; }

    /// <summary>Measures <c language="csharp">Guid.NewGuid().ToString("N")</c>.</summary>
    /// <returns>The identifier.</returns>
    [Benchmark(Baseline = true)]
    public string GuidSingle() => Guid.NewGuid().ToString("N");

    /// <summary>Measures a buffered random source with hex formatting.</summary>
    /// <returns>The identifier.</returns>
    [Benchmark]
    public string BufferedSingle() => NextBuffered();

    /// <summary>Measures <c language="csharp">Guid.NewGuid().ToString("N")</c> on several threads.</summary>
    [Benchmark]
    public void GuidParallel() => RunParallel(false);

    /// <summary>Measures the buffered random source on several threads.</summary>
    [Benchmark]
    public void BufferedParallel() => RunParallel(true);

    private static string NextBuffered()
    {
        var buffer = _buffer ??= new byte[BufferSize];
        if (_position == 0 || _position + IdBytes > BufferSize)
        {
            RandomNumberGenerator.Fill(buffer);
            _position = 0;
        }

        var id = Convert.ToHexStringLower(buffer.AsSpan(_position, IdBytes));
        _position += IdBytes;
        return id;
    }

    private static void GuidWork()
    {
        for (var n = 0; n < CallsPerThread; n++)
            _ = Guid.NewGuid().ToString("N");
    }

    private static void BufferedWork()
    {
        for (var n = 0; n < CallsPerThread; n++)
            _ = NextBuffered();
    }

    private void RunParallel(bool buffered)
    {
        var threads = new Thread[Threads];
        for (var i = 0; i < threads.Length; i++)
        {
            threads[i] = new Thread(buffered ? BufferedWork : GuidWork);
            threads[i].Start();
        }

        foreach (var thread in threads)
            thread.Join();
    }
}
