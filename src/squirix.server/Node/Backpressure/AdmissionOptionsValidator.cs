using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Backpressure;

[Immutable]
internal sealed class AdmissionOptionsValidator : ISettingsRule<AdmissionOptions>
{
    public IReadOnlyList<string> Check(AdmissionOptions options)
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
