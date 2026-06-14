using System;
using System.Text;
using System.Text.Json;
using Google.Protobuf;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Tests validating journal JSON codec metrics.
/// </summary>
public sealed class JournalJsonCodecMetricsTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures failed decode emits error op and duration metric.
    /// </summary>
    [Fact]
    public void DecodeErrorEmitsErrorOp()
    {
        using var sink = new MeasurementSink("Squirix");

        var garbage = new byte[] { 1, 2, 3, 4 };
        _ = Assert.ThrowsAny<Exception>(() => RecordCodec.Deserialize(garbage));

        Assert.True(sink.HasEvent("squirix_journal_json_ops_total", ("op", "decode"), ("result", "error")));
        Assert.True(sink.HasEvent("squirix_journal_json_op_duration_seconds", ("op", "decode")));
    }

    /// <summary>
    /// Ensures successful encode/decode emit ops, duration, and payload byte metrics.
    /// </summary>
    [Fact]
    public void EncodeDecodeSuccessEmitsOpsDurationAndPayloadBytes()
    {
        using var sink = new MeasurementSink("Squirix");

        var env = new JournalEnvelope
        {
            Seq = 1,
            UnixMs = 123,
            Put = new Put
            {
                Item = new EntryPair
                {
                    Key = "k1",
                    EntryJson = ByteString.CopyFromUtf8("{\"v\":1}"),
                },
            },
        };

        var bytes = RecordCodec.Serialize(env);
        Assert.NotEmpty(bytes);

        var env2 = RecordCodec.Deserialize(bytes);
        Assert.Equal(1UL, env2.Seq);

        Assert.True(sink.HasEvent("squirix_journal_json_ops_total", ("op", "encode"), ("result", "ok")));
        Assert.True(sink.HasEvent("squirix_journal_json_op_duration_seconds", ("op", "encode")));
        Assert.True(sink.HasEvent("squirix_journal_json_payload_bytes_total", ("op", "encode")));

        Assert.True(sink.HasEvent("squirix_journal_json_ops_total", ("op", "decode"), ("result", "ok")));
        Assert.True(sink.HasEvent("squirix_journal_json_op_duration_seconds", ("op", "decode")));
        Assert.True(sink.HasEvent("squirix_journal_json_payload_bytes_total", ("op", "decode")));
    }

    /// <summary>
    /// Ensures failed encode emits error op and duration metric.
    /// </summary>
    [Fact]
    public void EncodeErrorEmitsErrorOp()
    {
        using var sink = new MeasurementSink("Squirix");

        var env = new JournalEnvelope
        {
            Seq = 2,
            UnixMs = 456,
            Put = new Put(),
        };

        _ = Assert.Throws<InvalidOperationException>(() => RecordCodec.Serialize(env));

        Assert.True(sink.HasEvent("squirix_journal_json_ops_total", ("op", "encode"), ("result", "error")));
        Assert.True(sink.HasEvent("squirix_journal_json_op_duration_seconds", ("op", "encode")));
    }

    /// <summary>
    /// Ensures JSON journal encoding writes entry payloads as UTF-8 bytes instead of JSON strings.
    /// </summary>
    [Fact]
    public void EncodeWritesEntryPayloadAsUtf8Bytes()
    {
        const string entryJson = "{\"v\":{\"$t\":\"s\",\"v\":\"value\"},\"ver\":1}";
        var env = new JournalEnvelope
        {
            Seq = 1,
            UnixMs = 123,
            Put = new Put
            {
                Item = new EntryPair
                {
                    Key = "k1",
                    EntryJson = ByteString.CopyFromUtf8(entryJson),
                },
            },
        };

        var bytes = RecordCodec.Serialize(env);
        using var doc = JsonDocument.Parse(bytes);
        var item = doc.RootElement.GetProperty("put").GetProperty("item");

        Assert.False(item.TryGetProperty("entryJson", out _));
        Assert.True(item.TryGetProperty("entryJsonUtf8", out var entryJsonUtf8));
        Assert.Equal(Convert.ToBase64String(Encoding.UTF8.GetBytes(entryJson)), entryJsonUtf8.GetString());
        Assert.Equal(entryJson, RecordCodec.Deserialize(bytes).Put.Item.EntryJson.ToStringUtf8());
    }
}
