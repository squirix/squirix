namespace Squirix.Server.Storage.Replication;

/// <summary>Durability readiness state of a replica-group follower log.</summary>
internal enum FollowerLogReadiness
{
    /// <summary>The log has not completed startup validation.</summary>
    Unknown = 0,

    /// <summary>The committed prefix was validated and the log accepts ordered durable appends.</summary>
    Ready = 1,

    /// <summary>Startup validation found committed-prefix corruption; the log refuses appends and is closed.</summary>
    Failed = 2,
}
