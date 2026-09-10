using System;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Benchmarks;

/// <summary>Machine fingerprint for performance evidence percentage gates.</summary>
/// <remarks>
/// The fingerprint identifies a coarse host class, not an exact machine: CPU counts are bucketed and only the
/// runtime major/minor versions participate, so runner-size changes within a tier and patch updates do not
/// invalidate stored baselines. Exact-host gating would make evidence non-portable across a CI fleet.
/// </remarks>
[Immutable]
public static class MachineFingerprint
{
    /// <summary>Computes the stable machine fingerprint for the current host.</summary>
    /// <returns>Opaque fingerprint string identifying the benchmark host class.</returns>
    public static string Compute()
    {
        var builder = new StringBuilder(128);
        _ = builder.Append(ArchitectureToken(RuntimeInformation.OSArchitecture));
        _ = builder.Append('|');
        _ = builder.Append(ArchitectureToken(RuntimeInformation.ProcessArchitecture));
        _ = builder.Append('|');
        _ = builder.Append(CpuBucket(Environment.ProcessorCount));
        _ = builder.Append('|');
        _ = builder.Append(Environment.Version.Major.ToString(CultureInfo.InvariantCulture));
        _ = builder.Append('.');
        _ = builder.Append(Environment.Version.Minor.ToString(CultureInfo.InvariantCulture));
        _ = builder.Append('|');
        _ = builder.Append(OsFamilyToken());
        return builder.ToString();
    }

    /// <summary>Determines whether two fingerprints identify the same host class.</summary>
    /// <param name="left">First fingerprint.</param>
    /// <param name="right">Second fingerprint.</param>
    /// <returns>True when fingerprints match.</returns>
    public static bool Matches(string left, string right) => string.Equals(left, right, StringComparison.Ordinal);

    private static string ArchitectureToken(Architecture architecture) => architecture switch
    {
        Architecture.X64 => nameof(Architecture.X64),
        Architecture.X86 => nameof(Architecture.X86),
        Architecture.Arm => nameof(Architecture.Arm),
        Architecture.Arm64 => nameof(Architecture.Arm64),
        Architecture.Wasm => nameof(Architecture.Wasm),
        Architecture.S390x => nameof(Architecture.S390x),
        Architecture.LoongArch64 => nameof(Architecture.LoongArch64),
        Architecture.Armv6 => nameof(Architecture.Armv6),
        Architecture.Ppc64le => nameof(Architecture.Ppc64le),
        Architecture.RiscV64 => nameof(Architecture.RiscV64),

        // Future architectures fall into a shared coarse bucket instead of failing every perf gate:
        // exact-host gating would make evidence non-portable, and Enum.ToString is allocation-banned (ZA0802).
        _ => "unknown",
    };

    private static string CpuBucket(int processorCount) => processorCount switch
    {
        <= 4 => "cpu-4",
        <= 8 => "cpu-8",
        <= 16 => "cpu-16",
        <= 32 => "cpu-32",
        _ => "cpu-high",
    };

    private static string OsFamilyToken()
    {
        return true switch
        {
            _ when OperatingSystem.IsWindows() => "windows",
            _ when OperatingSystem.IsLinux() => "linux",
            _ when OperatingSystem.IsMacOS() => "osx",
            _ => "unknown",
        };
    }
}
