using System;

namespace Squirix.Server.TestKit.Diagnostics;

/// <summary>Best-effort logging for exceptions intentionally suppressed in test helpers.</summary>
internal static class TestLog
{
    internal static void Suppressed(string context, Exception exception) => Console.Error.WriteLine($"[squirix-testkit] {context}{Environment.NewLine}{exception}");
}
