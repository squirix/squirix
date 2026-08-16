using System;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;
using Squirix.Attributes;

namespace Squirix.Server.Node.MemoryPressure;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
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
