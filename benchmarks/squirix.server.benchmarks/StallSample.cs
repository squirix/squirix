namespace Squirix.Server.Benchmarks;

/// <summary>One timeline sample of the stall measurement.</summary>
/// <param name="TimeMs">Milliseconds since the run started.</param>
/// <param name="Waiting">Requests waiting for the mutation gate.</param>
/// <param name="Held">Requests holding the gate.</param>
/// <param name="Appended">Requests whose frame is appended and that wait for durability.</param>
/// <param name="Orphans">Requests the client already gave up on that the server still runs.</param>
/// <param name="ServerInflight">All requests the server still runs.</param>
/// <param name="ManagedBytes">The managed heap size.</param>
/// <param name="WorkingSetBytes">The process working set.</param>
/// <param name="IoAgeMs">The age of the journal I/O call in progress, as the stall probe reports it.</param>
internal sealed record StallSample(double TimeMs, int Waiting, int Held, int Appended, int Orphans, int ServerInflight, long ManagedBytes, long WorkingSetBytes, double IoAgeMs);
