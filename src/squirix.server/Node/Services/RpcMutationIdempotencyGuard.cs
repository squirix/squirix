using System;
using System.Collections.Concurrent;
using Google.Protobuf;
using Squirix.Server.Errors;

namespace Squirix.Server.Node.Services;

/// <summary>In-memory deduplication cache for mutating cache RPC outcomes.</summary>
internal sealed class RpcMutationIdempotencyGuard
{
    private readonly ConcurrentDictionary<string, StoredRpcOutcome> _records = new(StringComparer.Ordinal);
    private readonly TimeSpan _retention;

    public RpcMutationIdempotencyGuard(TimeSpan? retention = null)
    {
        _retention = retention ?? TimeSpan.FromMinutes(15);
    }

    public bool TryReplay<TResponse>(string operationId, string fingerprint, MessageParser<TResponse> parser, out TResponse? response)
        where TResponse : IMessage<TResponse>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(parser);

        SweepExpired(DateTime.UtcNow);

        if (!_records.TryGetValue(operationId, out var stored))
        {
            response = default;
            return false;
        }

        if (!string.Equals(stored.Fingerprint, fingerprint, StringComparison.Ordinal))
            throw new OperationIdReuseMismatchException();

        response = parser.ParseFrom(stored.ResponseBytes);
        return true;
    }

    public void RecordSuccess<TResponse>(string operationId, string fingerprint, TResponse response)
        where TResponse : IMessage<TResponse>
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(response);

        _records[operationId] = new StoredRpcOutcome(fingerprint, response.ToByteArray(), DateTime.UtcNow);
    }

    private void SweepExpired(DateTime utcNow)
    {
        foreach (var (key, value) in _records)
        {
            if (utcNow - value.CreatedUtc > _retention)
                _ = _records.TryRemove(key, out _);
        }
    }

    private sealed record StoredRpcOutcome(string Fingerprint, byte[] ResponseBytes, DateTime CreatedUtc);
}
