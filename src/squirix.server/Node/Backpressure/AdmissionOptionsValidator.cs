using System;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Backpressure;

[Immutable]
internal sealed class AdmissionOptionsValidator : IValidateOptions<AdmissionOptions>
{
    public ValidateOptionsResult Validate(string? name, AdmissionOptions options)
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
