namespace Squirix.Server.Logging;

/// <summary>Identifies an event in node diagnostics.</summary>
/// <param name="Id">Numeric identifier used by monitoring.</param>
/// <param name="Name">Typed event name.</param>
internal readonly record struct ServerLogEventId(int Id, string? Name = null);
