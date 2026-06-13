using JetBrains.Annotations;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Describes one journal frame preview.
/// </summary>
/// <param name="Sequence">The frame sequence.</param>
/// <param name="Operation">The operation kind.</param>
/// <param name="Key">The cache key, when available.</param>
/// <param name="CacheNamespace">The cache namespace, when available.</param>
/// <param name="Detail">Additional bounded operation detail.</param>
[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal readonly record struct JournalFramePreview(ulong Sequence, string Operation, string? Key, string? CacheNamespace, string? Detail);
