using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server;

/// <summary>Offline read-only replica diagnostics for the <c language="csharp">squirix-server doctor</c> command.</summary>
[Immutable]
public sealed class ReplicaDoctorReport
{
    /// <summary>Initializes a new instance of the <see cref="ReplicaDoctorReport" /> class.</summary>
    /// <param name="hasMismatch"><see langword="true" /> when any identity check disagrees with the configured topology.</param>
    /// <param name="lines">The stable diagnostic lines for doctor output.</param>
    public ReplicaDoctorReport(bool hasMismatch, IReadOnlyList<string> lines)
    {
        ArgumentNullException.ThrowIfNull(lines);
        HasMismatch = hasMismatch;
        Lines = [.. lines];
    }

    /// <summary>Gets a value indicating whether any identity check disagrees with the configured topology.</summary>
    public bool HasMismatch { get; }

    /// <summary>Gets the stable diagnostic lines for doctor output.</summary>
    public IReadOnlyList<string> Lines { get; }
}
