using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO;
using System.Text.Json;
using Squirix.Server.Attributes;
using Squirix.Server.Core;

namespace Squirix.Server.Node.Observability;

/// <summary>Decorator that records metrics for serialization operations and delegates to an inner serializer.</summary>
[Immutable]
internal sealed class ServerMetricsSerializer : IServerSerializer
{
    private const string OpDeserialize = "deserialize";

    private const string OpSerialize = "serialize";

    private readonly string _impl;
    private readonly IServerSerializer _inner;
    private readonly ServerSerializerMetrics _serializerMetrics;

    internal ServerMetricsSerializer(IServerSerializer inner, Meter meter)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _impl = _inner.GetType().Name;
        _serializerMetrics = new ServerSerializerMetrics(meter);
    }

    public T? Deserialize<T>(string payload)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = _inner.Deserialize<T>(payload);
            Record(OpDeserialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(OpDeserialize, ex, start))
        {
            throw;
        }
    }

    public T? Deserialize<T>(JsonElement payload)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = _inner.Deserialize<T>(payload);
            Record(OpDeserialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(OpDeserialize, ex, start))
        {
            throw;
        }
    }

    public T? Deserialize<T>(ReadOnlySpan<byte> payload)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = _inner.Deserialize<T>(payload);
            Record(OpDeserialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(OpDeserialize, ex, start))
        {
            throw;
        }
    }

    public T? Deserialize<T>(Stream payload)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = _inner.Deserialize<T>(payload);
            Record(OpDeserialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(OpDeserialize, ex, start))
        {
            throw;
        }
    }

    public void Serialize<T>(Stream destination, T? value)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            _inner.Serialize(destination, value);
            Record(OpSerialize, true, start);
        }
        catch (Exception ex) when (TryRecordSerializerFailure(OpSerialize, ex, start))
        {
            throw;
        }
    }

    public JsonElement SerializeToElement<T>(T? value)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = _inner.SerializeToElement(value);
            Record(OpSerialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(OpSerialize, ex, start))
        {
            throw;
        }
    }

    public byte[] SerializeToUtf8Bytes<T>(T? value)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = _inner.SerializeToUtf8Bytes(value);
            Record(OpSerialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(OpSerialize, ex, start))
        {
            throw;
        }
    }

    private void Record(string op, bool success, long startTimestamp)
    {
        var elapsedSeconds = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
        _serializerMetrics.OpsTotal.WithLabels(op, success ? "ok" : "error", _impl).Inc(1);
        _serializerMetrics.OpDurationSeconds.WithLabels(op, _impl).Observe(elapsedSeconds);
    }

    private void RecordFailure(string op, Exception ex, long startTimestamp)
    {
        var elapsedSeconds = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
        _serializerMetrics.OpsTotal.WithLabels(op, "error", _impl).Inc(1);
        _serializerMetrics.OpDurationSeconds.WithLabels(op, _impl).Observe(elapsedSeconds);
        var exType = ex.GetType().Name;
        _serializerMetrics.FailuresTotal.WithLabels(op, exType, _impl).Inc(1);
    }

    private bool TryRecordSerializerFailure(string op, Exception ex, long startTimestamp)
    {
        switch (ex)
        {
            case JsonException:
            case NotSupportedException:
            case InvalidOperationException:
            case IOException:
                RecordFailure(op, ex, startTimestamp);
                return true;
            default:
                return false;
        }
    }

    /// <summary>Metrics for serialization operations.</summary>
    [Immutable]
    private sealed class ServerSerializerMetrics
    {
        internal ServerSerializerMetrics(Meter meter)
        {
            FailuresTotal = new ServerCounter3Labels(meter.CreateCounter<long>("squirix_serializer_failures_total"), "op", "exception_type", "impl");
            OpDurationSeconds = new ServerHistogram2Labels(meter.CreateHistogram<double>("squirix_serializer_op_duration_seconds"), "op", "impl");
            OpsTotal = new ServerCounter3Labels(meter.CreateCounter<long>("squirix_serializer_ops_total"), "op", "result", "impl");
        }

        internal ServerCounter3Labels FailuresTotal { get; }

        internal ServerHistogram2Labels OpDurationSeconds { get; }

        internal ServerCounter3Labels OpsTotal { get; }
    }
}
