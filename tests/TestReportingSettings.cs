using System;
using TUnit.Core;

namespace Squirix.TestSettings;

/// <summary>Applies reporting settings shared by every Squirix test executable.</summary>
public static class TestReportingSettings
{
    /// <summary>
    /// Turns off the TUnit HTML and JSON reports. Neither is consumed anywhere, and producing them makes TUnit record
    /// every activity raised inside tests, which adds allocations and garbage collections. The TRX report is unaffected.
    /// </summary>
    /// <param name="context">The test discovery context that exposes the TUnit settings.</param>
    [Before(HookType.TestDiscovery)]
    public static void DisableReports(BeforeTestDiscoveryContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        context.Settings.Reporting.HtmlReportEnabled = false;
        context.Settings.Reporting.JsonReportEnabled = false;
    }
}
