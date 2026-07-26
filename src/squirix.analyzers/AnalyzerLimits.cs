namespace Squirix.Analyzers;

/// <summary>
/// Shared thresholds for Squirix design/naming analyzers (SQR002–SQR006).
/// SQR007 (type namespace prefix) has no numeric threshold.
/// </summary>
internal static class AnalyzerLimits
{
    /// <summary>Field name length (SQR006).</summary>
    internal const int MaxFieldNameLength = 50;

    /// <summary>Counted fields per type (SQR003).</summary>
    internal const int MaxFieldsPerType = 15;

    /// <summary>Method simple name length; property accessors subtract 4 (SQR005).</summary>
    internal const int MaxMethodNameLength = 50;

    /// <summary>Counted methods per type (SQR002).</summary>
    internal const int MaxMethodsPerType = 20;

    /// <summary>Type simple name length (SQR004).</summary>
    internal const int MaxTypeNameLength = 40;
}
