using System;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using Squirix.Server.Serialization;

namespace Squirix.Server.Node.Observability;

/// <summary>Decorator that records metrics for serialization operations and delegates to an inner serializer.</summary>
internal sealed class ServerMetricsSerializer : IServerSerializer
{
    private readonly string _impl;
    private readonly IServerSerializer _inner;

    public ServerMetricsSerializer(IServerSerializer inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _impl = _inner.GetType().Name;
    }

    public T? Deserialize<T>(string payload)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            var result = _inner.Deserialize<T>(payload);
            Record(ServerSerializerMetrics.OpDeserialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(ServerSerializerMetrics.OpDeserialize, ex, start))
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
            Record(ServerSerializerMetrics.OpDeserialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(ServerSerializerMetrics.OpDeserialize, ex, start))
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
            Record(ServerSerializerMetrics.OpDeserialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(ServerSerializerMetrics.OpDeserialize, ex, start))
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
            Record(ServerSerializerMetrics.OpDeserialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(ServerSerializerMetrics.OpDeserialize, ex, start))
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
            Record(ServerSerializerMetrics.OpSerialize, true, start);
        }
        catch (Exception ex) when (TryRecordSerializerFailure(ServerSerializerMetrics.OpSerialize, ex, start))
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
            Record(ServerSerializerMetrics.OpSerialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(ServerSerializerMetrics.OpSerialize, ex, start))
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
            Record(ServerSerializerMetrics.OpSerialize, true, start);
            return result;
        }
        catch (Exception ex) when (TryRecordSerializerFailure(ServerSerializerMetrics.OpSerialize, ex, start))
        {
            throw;
        }
    }

    private void Record(string op, bool success, long startTimestamp)
    {
        var elapsedSeconds = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
        ServerSerializerMetrics.OpsTotal.WithLabels(op, success ? "ok" : "error", _impl).Inc(1);
        ServerSerializerMetrics.OpDurationSeconds.WithLabels(op, _impl).Observe(elapsedSeconds);
    }

    private void RecordFailure(string op, Exception ex, long startTimestamp)
    {
        var elapsedSeconds = Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds;
        ServerSerializerMetrics.OpsTotal.WithLabels(op, "error", _impl).Inc(1);
        ServerSerializerMetrics.OpDurationSeconds.WithLabels(op, _impl).Observe(elapsedSeconds);
        var exType = ex.GetType().Name;
        ServerSerializerMetrics.FailuresTotal.WithLabels(op, exType, _impl).Inc(1);
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
}
