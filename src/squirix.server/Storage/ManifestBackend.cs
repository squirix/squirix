namespace Squirix.Server.Storage;

/// <summary>Selects the on-disk manifest store implementation (format + write path).</summary>
public enum ManifestBackend
{
    /// <summary>JSON <c>.msqx</c> files and a UTF-8 text <c>man-current</c> pointer.</summary>
    Json,

    /// <summary>Binary <c>.bmqx</c> files and a fixed-size <c>man-current</c> pointer; see docs/manifest-binary-format.md.</summary>
    Binary,
}
