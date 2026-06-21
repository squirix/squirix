using System;
using System.IO;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed.Json;
using Squirix.Server.Storage.JournalProto;

namespace Squirix.Server.Storage.Journaling.JsonFramed;

/// <summary>JSON frame body codec for JsonFramed journal (SJRN v1).</summary>
internal sealed class JsonFramedJournalCodec : IJournalFrameCodec
{
    public static JsonFramedJournalCodec Instance { get; } = new();

    public byte FileVersion => JournalFraming.Version;

    public JournalRecord Decode(ReadOnlySpan<byte> frameBody)
    {
        var env = RecordCodec.Deserialize(frameBody);
        return FromEnvelope(env);
    }

    public int Encode(JournalRecord record, Span<byte> destination) => throw new NotSupportedException("Legacy JSON-framed journal encoding is read-only.");

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
                        EntryJson = ByteString.CopyFrom(record.PutDiscriminatedEntryJson ?? []),
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
                var expiresMs = record.TouchExpirationUtc is { } utc ? new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeMilliseconds() : 0L;
                env.TouchExpiration = new TouchExpiration
                {
                    Key = record.Key.Key,
                    Namespace = record.Key.Namespace,
                    ExpiresUnixMs = expiresMs,
                };
                break;
            }

            case JournalOperationKind.AwaitDurabilityCommit:
            case JournalOperationKind.WaitForStartup:
            case JournalOperationKind.MaintenanceExclusive:
            case JournalOperationKind.SnapshotCut:
            case JournalOperationKind.UnderSnapshotBarrier:
            default:
                throw new NotSupportedException($"journal operation {record.Operation} cannot be converted to envelope.");
        }

        return env;
    }

    private static JournalRecord FromEnvelope(JournalEnvelope env)
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
                    Key = new CacheKey(put.Item.Namespace, put.Item.Key),
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
                    Key = new CacheKey(remove.Namespace, remove.Key),
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
                    Key = new CacheKey(removeExpiration.Namespace, removeExpiration.Key),
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
                    Key = new CacheKey(touch.Namespace, touch.Key),
                    TouchExpirationUtc = DateTimeOffset.FromUnixTimeMilliseconds(touch.ExpiresUnixMs).UtcDateTime,
                };
            }

            case JournalEnvelope.OpOneofCase.None:
            default:
                throw new InvalidDataException($"journal envelope has no supported op case ({env.OpCase}).");
        }
    }
}
