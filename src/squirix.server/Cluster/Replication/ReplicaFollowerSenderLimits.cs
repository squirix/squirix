using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The backlog and request size limits of a <see cref="ReplicaFollowerSender" />.</summary>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ReplicaFollowerSenderLimits
{
    /// <summary>Initializes a new instance of the <see cref="ReplicaFollowerSenderLimits" /> struct with the default limits.</summary>
    public ReplicaFollowerSenderLimits()
    {
    }

    /// <summary>Gets the most canonical payload bytes one request carries; 4 MiB unless set. A single larger entry still goes out alone.</summary>
    internal long MaxBatchBytes
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1L);

            field = value;
        }
    } = 4 * 1024 * 1024;

    /// <summary>Gets the most entries one request carries; 64 unless set.</summary>
    internal int MaxBatchEntries
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);

            field = value;
        }
    } = 64;

    /// <summary>Gets the most canonical payload bytes waiting to be sent; 64 MiB unless set. A single larger entry is still accepted while nothing else waits.</summary>
    internal long MaxPendingBytes
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1L);

            field = value;
        }
    } = 64L * 1024 * 1024;

    /// <summary>Gets the most entries waiting to be sent; <see cref="ReplicaFollowerSender.DefaultMaxPendingEntries" /> unless set.</summary>
    internal int MaxPendingEntries
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);

            field = value;
        }
    } = ReplicaFollowerSender.DefaultMaxPendingEntries;
}
