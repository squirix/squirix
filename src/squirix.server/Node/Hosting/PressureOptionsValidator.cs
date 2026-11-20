using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

[Immutable]
internal sealed class PressureOptionsValidator : ISettingsRule<PressureOptions>
{
    public IReadOnlyList<string> Check(PressureOptions options)
    {
        try
        {
            options.Validate();
            return [];
        }
        catch (InvalidOperationException ex)
        {
            return [ex.Message];
        }
    }
}
