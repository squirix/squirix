namespace Squirix.Server.Storage.Manifest;

/// <summary>Records manifest retention cleanup failures without coupling storage to product metrics.</summary>
internal interface IManifestRetentionFailureMetrics
{
    /// <summary>Records a failed retention delete or cleanup operation.</summary>
    /// <param name="artifactKind">Artifact kind label for the failed retention target.</param>
    /// <param name="outcome">Failure outcome label.</param>
    void RecordDeleteFailure(string artifactKind, string outcome);
}
