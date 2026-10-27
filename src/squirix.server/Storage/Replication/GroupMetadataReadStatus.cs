namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of reading a group metadata file.</summary>
internal enum GroupMetadataReadStatus
{
    /// <summary>The file holds a valid record.</summary>
    Valid = 0,

    /// <summary>The file has the expected layout but no slot can be trusted.</summary>
    Corrupt = 1,

    /// <summary>The file has a layout written by an earlier build, which is not read.</summary>
    Unsupported = 2,
}
