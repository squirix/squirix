using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.Versioning;
using System.Text;

namespace Squirix.Server.Utils;

/// <summary>Names the processes that hold a file open, to explain a Windows sharing violation.</summary>
internal static class FileHolderDiagnostics
{
    /// <summary>Maximum number of holders listed in a description.</summary>
    internal const int MaxHolders = 8;

    /// <summary>
    /// Size of the native <c language="csharp">RM_PROCESS_INFO</c> record: RM_UNIQUE_PROCESS (12 bytes: pid, start time), strAppName[256],
    /// strServiceShortName[64], then ApplicationType, AppStatus, TSSessionId, bRestartable (4 bytes each). Records are read from raw bytes
    /// because the assembly keeps runtime marshalling enabled, which source-generated imports do not support for struct buffers.
    /// </summary>
    private const int RecordSize = 668;
    private const int AppNameOffset = 12;
    private const int AppNameBytes = 512;
    private const int ServiceNameOffset = AppNameOffset + AppNameBytes;
    private const int ServiceNameBytes = 128;
    private const int ErrorMoreData = 234;
    private const int SessionKeyBytes = 66;
    private const int MaxQueryAttempts = 3;
    private const uint MaxQueryCapacity = 64;

    /// <summary>Describes the processes that currently hold <paramref name="path" /> open.</summary>
    /// <param name="path">The file path to inspect.</param>
    /// <returns>
    /// A short description such as <c language="csharp">MsMpEng.exe (pid 1234, service: WinDefend)</c> listing at most <see cref="MaxHolders" /> holders;
    /// <see langword="null" /> when not running on Windows, when no holder is reported, or when the Restart Manager fails.
    /// </returns>
    /// <remarks>
    /// Diagnostics only: this never throws for Restart Manager failures, which are reported as <see langword="null" />. It runs on
    /// failure paths only, so the cost of a Restart Manager session is acceptable.
    /// </remarks>
    internal static string? DescribeHolders(string path)
    {
        if (!OperatingSystem.IsWindows() || string.IsNullOrEmpty(path))
            return null;

        try
        {
            return QueryHolders(path);
        }
        catch (Exception ex) when (ex is DllNotFoundException or EntryPointNotFoundException or ArgumentException)
        {
            // The Restart Manager is unavailable or rejected the path; diagnostics must never mask the original failure.
            return null;
        }
    }

    private static string Format(ReadOnlySpan<byte> records, int count)
    {
        var builder = new StringBuilder();
        var listed = Math.Min(count, MaxHolders);
        for (var i = 0; i < listed; i++)
        {
            if (i > 0)
                _ = builder.Append("; ");

            var record = records.Slice(i * RecordSize, RecordSize);
            _ = builder.Append(ReadName(record.Slice(AppNameOffset, AppNameBytes))).Append(" (pid ").Append(BinaryPrimitives.ReadUInt32LittleEndian(record));
            var service = ReadName(record.Slice(ServiceNameOffset, ServiceNameBytes));
            if (service.Length > 0)
                _ = builder.Append(", service: ").Append(service);

            _ = builder.Append(')');
        }

        if (count > listed)
            _ = builder.Append("; and ").Append(count - listed).Append(" more");

        return builder.ToString();
    }

    [SupportedOSPlatform("windows")]
    private static string? ListHolders(uint session)
    {
        var capacity = 1u;
        for (var attempt = 0; attempt < MaxQueryAttempts; attempt++)
        {
            var records = ArrayPool<byte>.Shared.Rent(int.CreateChecked(capacity) * RecordSize);
            try
            {
                var count = capacity;
                var result = NativeMethods.RestartManagerGetList(session, out var needed, ref count, records, out _);
                if (result == 0)
                    return count == 0 ? null : Format(records, int.CreateChecked(count));

                if (result != ErrorMoreData || needed > MaxQueryCapacity)
                    return null;

                capacity = needed;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(records);
            }
        }

        return null;
    }

    [SupportedOSPlatform("windows")]
    private static string? QueryHolders(string path)
    {
        var key = ArrayPool<byte>.Shared.Rent(SessionKeyBytes);
        try
        {
            if (NativeMethods.RestartManagerStartSession(out var session, 0, key) != 0)
                return null;

            try
            {
                // Win32 errors of the Restart Manager are swallowed deliberately: a failed query only means "holder unknown".
                return NativeMethods.RestartManagerRegisterFiles(session, 1, [path], 0, 0, 0, 0) == 0 ? ListHolders(session) : null;
            }
            finally
            {
                _ = NativeMethods.RestartManagerEndSession(session);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(key);
        }
    }

    private static string ReadName(ReadOnlySpan<byte> utf16)
    {
        var name = Encoding.Unicode.GetString(utf16);
        var end = name.IndexOf('\0', StringComparison.Ordinal);
        return end < 0 ? name : name[..end];
    }
}
