using System;
using System.Diagnostics;
using System.Text.Json;
using Google.Protobuf;
using Squirix.Server.Node.Observability;
using Squirix.Server.Serialization;
using Squirix.Server.Storage.JournalProto;

namespace Squirix.Server.Storage.Journaling.Json;

internal static class RecordCodec
{
    public static JournalEnvelope Deserialize(ReadOnlySpan<byte> utf8Json)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var dto = JsonSerializer.Deserialize<RecordEnvelope>(utf8Json, DurabilityJson.StrictSerializerOptions) ??
                      throw new JsonException("Failed to deserialize RecordEnvelope");
            ValidateDto(dto);
            var env = FromDto(dto);
            JournalJsonCodecMetrics.AddOp("decode", "ok");
            JournalJsonCodecMetrics.AddPayloadBytes("decode", utf8Json.Length);
            JournalJsonCodecMetrics.RecordDuration("decode", sw.Elapsed.TotalSeconds);
            return env;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or InvalidOperationException or NullReferenceException)
        {
            RecordErrorMetrics("decode", sw);
            throw;
        }
    }

    public static byte[] Serialize(JournalEnvelope env)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(ToDto(env), SquirixJsonSerializerContext.Default.RecordEnvelope);
            JournalJsonCodecMetrics.AddOp("encode", "ok");
            JournalJsonCodecMetrics.AddPayloadBytes("encode", bytes.Length);
            JournalJsonCodecMetrics.RecordDuration("encode", sw.Elapsed.TotalSeconds);
            return bytes;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or InvalidOperationException or NullReferenceException)
        {
            RecordErrorMetrics("encode", sw);
            throw;
        }
    }

    private static JournalEnvelope FromDto(RecordEnvelope dto)
    {
        var env = new JournalEnvelope
        {
            Seq = dto.Seq,
            UnixMs = dto.UnixMs,
        };

        if (dto.Put != null)
        {
            env.Put = new Put
            {
                Item = new EntryPair
                {
                    Key = dto.Put.Item.Key,
                    Namespace = dto.Put.Item.Namespace ?? string.Empty,
                    EntryJson = ToByteString(dto.Put.Item.EntryJsonUtf8),
                },
                OperationId = dto.Put.OperationId ?? string.Empty,
            };
            return env;
        }

        if (dto.Remove != null)
        {
            env.Remove = new Remove { Key = dto.Remove.Key, Namespace = dto.Remove.Namespace ?? string.Empty };
            return env;
        }

        if (dto.RemoveExpiration != null)
        {
            env.RemoveExpiration = new RemoveExpiration { Key = dto.RemoveExpiration.Key, Namespace = dto.RemoveExpiration.Namespace ?? string.Empty };
            return env;
        }

        if (dto.TouchExpiration != null)
        {
            env.TouchExpiration = new TouchExpiration
            {
                Key = dto.TouchExpiration.Key,
                Namespace = dto.TouchExpiration.Namespace ?? string.Empty,
                ExpiresUnixMs = dto.TouchExpiration.ExpiresUnixMs,
            };
        }

        return env;
    }

    private static void RecordErrorMetrics(string operation, Stopwatch sw)
    {
        JournalJsonCodecMetrics.AddOp(operation, "error");
        JournalJsonCodecMetrics.RecordDuration(operation, sw.Elapsed.TotalSeconds);
    }

    private static ByteString ToByteString(byte[]? utf8) => utf8 is { Length: > 0 } ? UnsafeByteOperations.UnsafeWrap(utf8) : ByteString.Empty;

    private static RecordEnvelope ToDto(JournalEnvelope env)
    {
        var dto = new RecordEnvelope
        {
            Seq = env.Seq,
            UnixMs = env.UnixMs,
        };

        switch (env.OpCase)
        {
            case JournalEnvelope.OpOneofCase.Put:
            {
                var put = env.Put ?? throw new InvalidOperationException("journal envelope op case is Put but payload is missing.");
                dto.Put = new PutOp
                {
                    OperationId = put.OperationId,
                    Item = new ItemPair
                    {
                        Key = put.Item.Key,
                        Namespace = string.IsNullOrEmpty(put.Item.Namespace) ? null : put.Item.Namespace,
                        EntryJsonUtf8 = put.Item.EntryJson.ToByteArray(),
                    },
                };
                break;
            }

            case JournalEnvelope.OpOneofCase.Remove:
            {
                var remove = env.Remove ?? throw new InvalidOperationException("journal envelope op case is Remove but payload is missing.");
                dto.Remove = new RemoveOp
                {
                    Key = remove.Key,
                    Namespace = string.IsNullOrEmpty(remove.Namespace) ? null : remove.Namespace,
                };
                break;
            }

            case JournalEnvelope.OpOneofCase.RemoveExpiration:
            {
                var removeExpiration = env.RemoveExpiration ?? throw new InvalidOperationException("journal envelope op case is RemoveExpiration but payload is missing.");
                dto.RemoveExpiration = new RemoveExpirationOp
                {
                    Key = removeExpiration.Key,
                    Namespace = string.IsNullOrEmpty(removeExpiration.Namespace) ? null : removeExpiration.Namespace,
                };
                break;
            }

            case JournalEnvelope.OpOneofCase.TouchExpiration:
            {
                var touchExpiration = env.TouchExpiration ?? throw new InvalidOperationException("journal envelope op case is TouchExpiration but payload is missing.");
                dto.TouchExpiration = new TouchExpirationOp
                {
                    Key = touchExpiration.Key,
                    Namespace = string.IsNullOrEmpty(touchExpiration.Namespace) ? null : touchExpiration.Namespace,
                    ExpiresUnixMs = touchExpiration.ExpiresUnixMs,
                };
                break;
            }

            case JournalEnvelope.OpOneofCase.None:
            default:
                break;
        }

        return dto;
    }

    private static void ValidateDto(RecordEnvelope dto)
    {
        var opCount = 0;
        if (dto.Put is not null)
            opCount++;
        if (dto.Remove is not null)
            opCount++;
        if (dto.RemoveExpiration is not null)
            opCount++;
        if (dto.TouchExpiration is not null)
            opCount++;

        if (opCount != 1)
            throw new JsonException($"journal envelope must contain exactly one operation, but found {opCount}.");

        if (dto.Put is not null)
            ValidatePut(dto.Put);
        if (dto.Remove is not null)
            ValidateKey(dto.Remove.Key, "remove.key");
        if (dto.RemoveExpiration is not null)
            ValidateKey(dto.RemoveExpiration.Key, "removeExpiration.key");
        if (dto.TouchExpiration is null)
            return;
        ValidateKey(dto.TouchExpiration.Key, "touchExpiration.key");
        if (dto.TouchExpiration.ExpiresUnixMs <= 0)
            throw new JsonException("journal touchExpiration.expiresUnixMs must be positive.");
    }

    private static void ValidateKey(string? key, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new JsonException($"journal {fieldName} is missing.");
    }

    private static void ValidatePut(PutOp put)
    {
        if (put.Item is null)
            throw new JsonException("journal put.item is missing.");

        ValidateKey(put.Item.Key, "put.item.key");
        if (put.Item.EntryJsonUtf8 is null)
            throw new JsonException("journal put.item.entryJsonUtf8 is missing.");
    }
}
