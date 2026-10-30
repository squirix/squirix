using System;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.Node.Hosting;

[Immutable]
internal sealed class PressureOptionsValidator : IValidateOptions<PressureOptions>
{
    public ValidateOptionsResult Validate(string? name, PressureOptions options)
    {
        try
        {
            options.Validate();
            return ValidateOptionsResult.Success;
        }
        catch (InvalidOperationException ex)
        {
            return ValidateOptionsResult.Fail(ex.Message);
        }
    }
}
