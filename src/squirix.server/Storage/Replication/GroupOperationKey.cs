using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Identity of a retained group idempotency record.</summary>
/// <param name="Scope">The operation scope.</param>
/// <param name="OperationId">The operation identifier.</param>
[Immutable]
internal readonly record struct GroupOperationKey(string Scope, string OperationId)
{
    /// <summary>Returns the identity of a record.</summary>
    /// <param name="record">The record.</param>
    /// <returns>Its <c language="csharp">(scope, operation id)</c> key.</returns>
    internal static GroupOperationKey Of(in GroupIdempotencyRecord record) => new(record.OperationScope, record.OperationId);
}
