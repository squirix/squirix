using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
[Immutable]
internal sealed class PersistenceOptionsValidator : IValidateOptions<PersistenceOptions>
{
    public ValidateOptionsResult Validate(string? name, PersistenceOptions options)
    {
        var failures = new List<string>();
        if (string.IsNullOrWhiteSpace(options.DataDir))
            failures.Add("Persistence DataDir is required.");

        try
        {
            options.Validate();
        }
        catch (InvalidOperationException ex)
        {
            failures.Add(ex.Message);
        }

        return OptionsValidator.ToResult(failures);
    }
}
