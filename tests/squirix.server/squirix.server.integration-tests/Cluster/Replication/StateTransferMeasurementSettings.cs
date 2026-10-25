using System;
using System.Globalization;
using Squirix.Server.Attributes;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>The knobs of the opt-in state transfer measurement, read from environment variables; the measurement runs only when the key count is set.</summary>
[Immutable]
internal sealed class StateTransferMeasurementSettings
{
    /// <summary>The variable that names the number of keys to transfer and switches the measurement on.</summary>
    internal const string KeysVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_KEYS";

    private const string ValueBytesVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_VALUE_BYTES";
    private const string WritersVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_WRITERS";
    private const string FillWritersVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_FILL_WRITERS";
    private const string BaselineSecondsVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_BASELINE_SECONDS";
    private const string FollowerJournalMbVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_FOLLOWER_JOURNAL_MB";
    private const string FollowerMaxCacheMbVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_FOLLOWER_MAX_CACHE_MB";
    private const string ReadyTimeoutSecondsVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_READY_TIMEOUT_SECONDS";
    private const string RepeatsVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_REPEATS";
    private const string ReportVariable = "SQUIRIX_MEASURE_STATE_TRANSFER_REPORT";

    private const int DefaultValueBytes = 100;
    private const int DefaultWriters = 8;
    private const int DefaultFillWriters = 64;
    private const int DefaultBaselineSeconds = 10;
    private const int DefaultReadyTimeoutSeconds = 10_800;

    /// <summary>Gets the number of keys the fill writes while the follower is down.</summary>
    internal required int Keys { get; init; }

    /// <summary>Gets the size of every value in bytes.</summary>
    internal required int ValueBytes { get; init; }

    /// <summary>Gets the number of concurrent writers before and during the transfer.</summary>
    internal required int Writers { get; init; }

    /// <summary>Gets the number of concurrent writers of the fill.</summary>
    internal required int FillWriters { get; init; }

    /// <summary>Gets the length of the write baseline with every member up.</summary>
    internal required TimeSpan Baseline { get; init; }

    /// <summary>Gets how long the returned follower may take to be admitted again before the run gives up and reports what it saw.</summary>
    internal required TimeSpan ReadyTimeout { get; init; }

    /// <summary>Gets how many transfers the run makes on the same cluster; every one after the first repeats the lag of the follower with a short write run.</summary>
    internal required int Repeats { get; init; }

    /// <summary>Gets the journal quota in megabytes the returned follower starts with, or <see langword="null" /> to keep the quota of the other nodes.</summary>
    internal int? FollowerJournalMb { get; init; }

    /// <summary>Gets the memory pressure budget in megabytes of the returned follower, or <see langword="null" /> to keep the default budget.</summary>
    internal long? FollowerMaxCacheMb { get; init; }

    /// <summary>Gets the file the report is appended to, or <see langword="null" /> to print it only.</summary>
    internal string? ReportPath { get; init; }

    /// <summary>Reads the settings from the environment.</summary>
    /// <returns>The settings; <see langword="null" /> when the measurement is not switched on.</returns>
    internal static StateTransferMeasurementSettings? FromEnvironment()
    {
        var keys = ReadInt(KeysVariable);
        return keys > 0 ? Build(keys.Value) : null;
    }

    private static StateTransferMeasurementSettings Build(int keys) =>
        new()
        {
            Keys = keys,
            ValueBytes = ReadInt(ValueBytesVariable) ?? DefaultValueBytes,
            Writers = ReadInt(WritersVariable) ?? DefaultWriters,
            FillWriters = ReadInt(FillWritersVariable) ?? DefaultFillWriters,
            Baseline = TimeSpan.FromSeconds(ReadInt(BaselineSecondsVariable) ?? DefaultBaselineSeconds),
            ReadyTimeout = TimeSpan.FromSeconds(ReadInt(ReadyTimeoutSecondsVariable) ?? DefaultReadyTimeoutSeconds),
            Repeats = Math.Max(1, ReadInt(RepeatsVariable) ?? 1),
            FollowerJournalMb = ReadInt(FollowerJournalMbVariable),
            FollowerMaxCacheMb = ReadInt(FollowerMaxCacheMbVariable),
            ReportPath = Environment.GetEnvironmentVariable(ReportVariable),
        };

    private static int? ReadInt(string name) =>
        int.TryParse(Environment.GetEnvironmentVariable(name), NumberStyles.None, CultureInfo.InvariantCulture, out var value) ? value : null;
}
