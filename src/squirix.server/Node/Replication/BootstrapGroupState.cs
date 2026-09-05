namespace Squirix.Server.Node.Replication;

/// <summary>Durable stopped-cluster bootstrap progress for one replica group.</summary>
internal enum BootstrapGroupState : byte
{
    /// <summary>The group has not yet been prepared.</summary>
    Pending = 0,

    /// <summary>Source material has been prepared.</summary>
    Prepared = 1,

    /// <summary>Replica material has been installed.</summary>
    Installed = 2,

    /// <summary>The installed replica has been verified.</summary>
    Verified = 3,
}
