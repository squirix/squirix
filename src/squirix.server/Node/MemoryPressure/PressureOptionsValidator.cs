using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Node.MemoryPressure;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
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
