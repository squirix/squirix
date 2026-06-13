using JetBrains.Annotations;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Describes read-only verification of a storage artifact.
/// </summary>
/// <param name="IsValid">Whether the artifact is valid.</param>
/// <param name="Summary">A short verification summary.</param>
/// <param name="Error">The validation failure reason, when available.</param>
[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal readonly record struct StorageVerificationResult(bool IsValid, string Summary, string? Error);
