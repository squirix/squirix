namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Ring distribution sample for admin diagnostics.
/// </summary>
internal readonly record struct AdminRingOwnerSample(string Key, string Owner);
