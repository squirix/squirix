using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>The records of the idempotency store by operation identifier, with the bytes they are charged kept in step.</summary>
/// <remarks>Not thread-safe: the owning store calls it under its gate.</remarks>
[Mutable]
internal sealed class IdempotencyRecordMap
{
    private readonly Dictionary<string, IdempotencyStoredRecord> _records = [with(StringComparer.Ordinal)];

    /// <summary>Gets the bytes the records are charged: <see cref="IdempotencyOptions.RecordOverheadBytes" /> and the response of each.</summary>
    internal long Bytes { get; private set; }

    /// <summary>Gets the number of records.</summary>
    internal int Count => _records.Count;

    /// <summary>Sets the record of an identifier, giving back the charge of the record it replaces.</summary>
    /// <param name="operationId">The operation identifier.</param>
    internal IdempotencyStoredRecord this[string operationId]
    {
        set
        {
            if (_records.TryGetValue(operationId, out var previous))
                Bytes -= previous.Charge;

            _records[operationId] = value;
            Bytes += value.Charge;
        }
    }

    /// <summary>Gets the enumerator of the records, in no particular order.</summary>
    /// <returns>The enumerator.</returns>
    public Dictionary<string, IdempotencyStoredRecord>.Enumerator GetEnumerator() => _records.GetEnumerator();

    /// <summary>Tells whether an identifier holds a record.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns><see langword="true" /> when it does.</returns>
    internal bool ContainsKey(string operationId) => _records.ContainsKey(operationId);

    /// <summary>Removes the record of an identifier and gives its charge back.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns><see langword="true" /> when the identifier held a record.</returns>
    internal bool Remove(string operationId)
    {
        if (!_records.Remove(operationId, out var removed))
            return false;

        Bytes -= removed.Charge;
        return true;
    }

    /// <summary>Looks up the record of an identifier.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="record">The record when found; otherwise <see langword="default" />.</param>
    /// <returns><see langword="true" /> when the identifier holds a record.</returns>
    internal bool TryGetValue(string operationId, out IdempotencyStoredRecord record) => _records.TryGetValue(operationId, out record);
}
