namespace Squirix.Server.Storage.Snapshot;

/// <summary>Evaluates whether background snapshot work should be suppressed due to node resource pressure.</summary>
internal interface IBackgroundSnapshotMemoryThrottle
{
    /// <summary>Gets a value indicating whether a background snapshot attempt should be skipped.</summary>
    /// <returns><see langword="true" /> when background snapshot work should be suppressed; otherwise <see langword="false" />.</returns>
    bool ShouldSuppressBackgroundSnapshot();
}
