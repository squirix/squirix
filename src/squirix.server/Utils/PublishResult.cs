namespace Squirix.Server.Utils;

/// <summary>Outcome of a successful <see cref="FileEx.PublishFile" />.</summary>
/// <param name="Attempts">The number of attempts the publication needed; greater than one means a transient sharing failure was waited out.</param>
/// <param name="Holders">A description of the processes that held the destination during a sharing failure, or <see langword="null" /> when none was observed or known.</param>
internal readonly record struct PublishResult(int Attempts, string? Holders);
