using System;
using System.IO;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Codec;

/// <summary>JSON frame body codec for JsonFramed WAL (SJRN v1).</summary>
internal sealed class JsonFramedJournalCodec : IJournalFrameCodec
{
    public static JsonFramedJournalCodec Instance { get; } = new();

    public byte FileVersion => JournalFraming.Version;

    public int Encode(JournalRecord record, Span<byte> destination) =>
        throw new NotSupportedException("JsonFramed encoding uses RecordCodec via JournalWriter.");

    public JournalRecord Decode(ReadOnlySpan<byte> frameBody)
    {
        var env = RecordCodec.Deserialize(frameBody);
        return FromEnvelope(env);
    }

    internal static JournalRecord FromEnvelope(JournalEnvelope env)
    {
        switch (env.OpCase)
        {
            case JournalEnvelope.OpOneofCase.Put:
            {
                var put = env.Put ?? throw new InvalidDataException("journal envelope op case is Put but payload is missing.");
                return new JournalRecord
                {
                    Sequence = env.Seq,
                    UnixMs = env.UnixMs,
                    Operation = JournalOperationKind.Put,
                    Key = new Core.CacheKey(put.Item.Namespace, put.Item.Key),
                    PutDiscriminatedEntryJson = put.Item.EntryJson.ToByteArray(),
                    PutOperationId = put.OperationId,
                };
            }

            case JournalEnvelope.OpOneofCase.Remove:
            {
                var remove = env.Remove ?? throw new InvalidDataException("journal envelope op case is Remove but payload is missing.");
                return new JournalRecord
                {
                    Sequence = env.Seq,
                    UnixMs = env.UnixMs,
                    Operation = JournalOperationKind.Remove,
                    Key = new Core.CacheKey(remove.Namespace, remove.Key),
                };
            }

            case JournalEnvelope.OpOneofCase.RemoveExpiration:
            {
                var removeExpiration = env.RemoveExpiration ?? throw new InvalidDataException("journal envelope op case is RemoveExpiration but payload is missing.");
                return new JournalRecord
                {
                    Sequence = env.Seq,
                    UnixMs = env.UnixMs,
                    Operation = JournalOperationKind.RemoveExpiration,
                    Key = new Core.CacheKey(removeExpiration.Namespace, removeExpiration.Key),
                };
            }

            case JournalEnvelope.OpOneofCase.TouchExpiration:
            {
                var touch = env.TouchExpiration ?? throw new InvalidDataException("journal envelope op case is TouchExpiration but payload is missing.");
                return new JournalRecord
                {
                    Sequence = env.Seq,
                    UnixMs = env.UnixMs,
                    Operation = JournalOperationKind.TouchExpiration,
                    Key = new Core.CacheKey(touch.Namespace, touch.Key),
                    TouchExpirationUtc = DateTimeOffset.FromUnixTimeMilliseconds(touch.ExpiresUnixMs).UtcDateTime,
                };
            }

            default:
                throw new InvalidDataException($"journal envelope has no supported op case ({env.OpCase}).");
        }
    }

    internal static JournalEnvelope ToEnvelope(JournalRecord record)
    {
        var env = new JournalEnvelope { Seq = record.Sequence, UnixMs = record.UnixMs };
        switch (record.Operation)
        {
            case JournalOperationKind.Put:
                env.Put = new Put
                {
                    Item = new EntryPair
                    {
                        Key = record.Key.Key,
                        Namespace = record.Key.Namespace,
                        EntryJson = Google.Protobuf.ByteString.CopyFrom(record.PutDiscriminatedEntryJson ?? []),
                    },
                    OperationId = record.PutOperationId ?? string.Empty,
                };
                break;

            case JournalOperationKind.Remove:
                env.Remove = new Remove { Key = record.Key.Key, Namespace = record.Key.Namespace };
                break;

            case JournalOperationKind.RemoveExpiration:
                env.RemoveExpiration = new RemoveExpiration { Key = record.Key.Key, Namespace = record.Key.Namespace };
                break;

            case JournalOperationKind.TouchExpiration:
            {
                var expiresMs = record.TouchExpirationUtc is { } utc
                    ? new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeMilliseconds()
                    : 0L;
                env.TouchExpiration = new TouchExpiration
                {
                    Key = record.Key.Key,
                    Namespace = record.Key.Namespace,
                    ExpiresUnixMs = expiresMs,
                };
                break;
            }

            default:
                throw new NotSupportedException($"journal operation {record.Operation} cannot be converted to envelope.");
        }

        return env;
    }
}
