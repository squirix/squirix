using System;

namespace Squirix.Server.Benchmarks;

/// <summary>The server-side progress of one durable mutation of the stall measurement; members are guarded by locking the instance where races exist.</summary>
internal sealed class StallOp
{
    /// <summary>Gets the request this operation belongs to.</summary>
    internal required StallRequest Request { get; init; }

    /// <summary>Gets or sets a value indicating whether the frame was appended to the ring.</summary>
    internal bool Appended { get; set; }

    /// <summary>Gets or sets a value indicating whether the server finished with the operation.</summary>
    internal bool Done { get; set; }

    /// <summary>Gets or sets a value indicating whether the operation took the mutation gate.</summary>
    internal bool EnteredGate { get; set; }

    /// <summary>Gets or sets the failure, when the operation failed.</summary>
    internal Exception? Failure { get; set; }

    /// <summary>Gets or sets a value indicating whether the client gave up while the operation was still running.</summary>
    internal bool GaveUp { get; set; }

    /// <summary>Gets or sets when the operation took the mutation gate.</summary>
    internal double GateEnterMs { get; set; }

    /// <summary>Gets or sets a value indicating whether the gate hold was already recorded.</summary>
    internal bool HoldEnded { get; set; }

    /// <summary>Gets or sets a value indicating whether the operation committed.</summary>
    internal bool Succeeded { get; set; }
}
