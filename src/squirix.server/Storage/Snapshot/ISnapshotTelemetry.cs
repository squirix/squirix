using System;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>Records snapshot tracing and metrics without coupling storage to product observability types.</summary>
internal interface ISnapshotTelemetry
{
    /// <summary>Begins a trace scope for snapshot creation when tracing is enabled.</summary>
    /// <returns>An active scope, or <see langword="null" /> when tracing is disabled.</returns>
    ISnapshotTraceScope? BeginCreate();

    /// <summary>Records snapshot duration for the given node and outcome.</summary>
    /// <param name="nodeId">Node identifier label.</param>
    /// <param name="result">Outcome label (<c language="csharp">success</c> or <c language="csharp">failure</c>).</param>
    /// <param name="elapsed">Observed snapshot duration.</param>
    void RecordDuration(string nodeId, string result, TimeSpan elapsed);
}
