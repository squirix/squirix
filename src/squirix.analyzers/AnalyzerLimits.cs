namespace Squirix.Analyzers;

/// <summary>
/// Shared thresholds for Squirix design/naming analyzers (SQR002–SQR006).
/// </summary>
internal static class AnalyzerLimits
{
    /// <summary>Counted methods per type (SQR002).</summary>
    internal const int MaxMethodsPerType = 20;

    /// <summary>Counted fields per type (SQR003).</summary>
    internal const int MaxFieldsPerType = 15;

    /// <summary>Type simple name length (SQR004).</summary>
    internal const int MaxTypeNameLength = 40;

    /// <summary>Method simple name length; property accessors subtract 4 (SQR005).</summary>
    internal const int MaxMethodNameLength = 50;

    /// <summary>Field name length (SQR006).</summary>
    internal const int MaxFieldNameLength = 50;
}
