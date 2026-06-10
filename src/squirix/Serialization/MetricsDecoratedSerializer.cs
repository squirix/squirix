using System;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using Squirix.Internal.Cluster.Observability;

namespace Squirix.Serialization;

/// <summary>
/// Decorator that records metrics for serialization operations and delegates to an inner serializer.
/// </summary>
internal sealed class MetricsDecoratedSerializer : ISquirixSerializer
{
    private readonly string _impl;

    private readonly ISquirixSerializer _inner;

    public MetricsDecoratedSerializer(ISquirixSerializer inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _impl = _inner.GetType().Name;
    }

    public T? Deserialize<T>(string payload) => Measure("deserialize", () => _inner.Deserialize<T>(payload));

    public T? Deserialize<T>(JsonElement payload) => Measure("deserialize", () => _inner.Deserialize<T>(payload));

    public T? Deserialize<T>(ReadOnlySpan<byte> payload)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = _inner.Deserialize<T>(payload);
            Record("deserialize", true, start);
            return result;
        }
        catch (JsonException ex)
        {
            RecordFailure("deserialize", ex, start);
            throw;
        }
        catch (NotSupportedException ex)
        {
            RecordFailure("deserialize", ex, start);
            throw;
        }
        catch (InvalidOperationException ex)
        {
            RecordFailure("deserialize", ex, start);
            throw;
        }
        catch (IOException ex)
        {
            RecordFailure("deserialize", ex, start);
            throw;
        }
    }

    public T? Deserialize<T>(Stream payload) => Measure("deserialize", () => _inner.Deserialize<T>(payload));

    public void Serialize<T>(Stream destination, T? value)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            _inner.Serialize(destination, value);
            Record("serialize", true, start);
        }
        catch (JsonException ex)
        {
            RecordFailure("serialize", ex, start);
            throw;
        }
        catch (NotSupportedException ex)
        {
            RecordFailure("serialize", ex, start);
            throw;
        }
        catch (InvalidOperationException ex)
        {
            RecordFailure("serialize", ex, start);
            throw;
        }
        catch (IOException ex)
        {
            RecordFailure("serialize", ex, start);
            throw;
        }
    }

    public JsonElement SerializeToElement<T>(T? value) => Measure("serialize", () => _inner.SerializeToElement(value));

    public byte[] SerializeToUtf8Bytes<T>(T? value) => Measure("serialize", () => _inner.SerializeToUtf8Bytes(value));

    private TResult Measure<TResult>(string op, Func<TResult> func)
    {
        var sw = Stopwatch.GetTimestamp();
        try
        {
            var result = func();
            Record(op, true, sw);
            return result;
        }
        catch (JsonException ex)
        {
            RecordFailure(op, ex, sw);
            throw;
        }
        catch (NotSupportedException ex)
        {
            RecordFailure(op, ex, sw);
            throw;
        }
        catch (InvalidOperationException ex)
        {
            RecordFailure(op, ex, sw);
            throw;
        }
        catch (IOException ex)
        {
            RecordFailure(op, ex, sw);
            throw;
        }
    }

    private void Record(string op, bool success, long startTimestamp)
    {
        var elapsedSeconds = (Stopwatch.GetTimestamp() - startTimestamp) / (double)Stopwatch.Frequency;
        SerializerMetrics.OpsTotal.WithLabels(op, success ? "ok" : "error", _impl).Inc(1);
        SerializerMetrics.OpDurationSeconds.WithLabels(op, _impl).Observe(elapsedSeconds);
    }

    private void RecordFailure(string op, Exception ex, long startTimestamp)
    {
        var elapsedSeconds = (Stopwatch.GetTimestamp() - startTimestamp) / (double)Stopwatch.Frequency;
        SerializerMetrics.OpsTotal.WithLabels(op, "error", _impl).Inc(1);
        SerializerMetrics.OpDurationSeconds.WithLabels(op, _impl).Observe(elapsedSeconds);
        var exType = ex.GetType().Name;
        SerializerMetrics.FailuresTotal.WithLabels(op, exType, _impl).Inc(1);
    }
}
