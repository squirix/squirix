using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of installing a replica-group snapshot.</summary>
/// <param name="Success">Determines whether the snapshot was installed.</param>
/// <param name="RefusalCode">The stable refusal marker when the install was rejected; otherwise empty.</param>
/// <remarks>
///     <para>
///     The positional <see cref="RefusalCode" /> is nullable because a default-constructed instance bypasses
///     both factories and would otherwise surface a null refusal marker to consumers.
///     </para>
/// </remarks>
[Immutable]
internal readonly record struct GroupSnapshotInstallResult(bool Success, string? RefusalCode)
{
    /// <summary>Gets the stable refusal marker with the default-instance null normalized to empty.</summary>
    internal string Refusal => RefusalCode ?? string.Empty;

    /// <summary>Gets the accepted install outcome.</summary>
    internal static GroupSnapshotInstallResult Installed { get; } = new(true, string.Empty);

    /// <summary>Creates a refusal outcome with a stable refusal marker.</summary>
    /// <param name="refusalCode">The stable refusal marker.</param>
    /// <returns>The refusal outcome.</returns>
    internal static GroupSnapshotInstallResult Refused(string refusalCode)
    {
        ArgumentException.ThrowIfNullOrEmpty(refusalCode);
        return new GroupSnapshotInstallResult(false, refusalCode);
    }
}
