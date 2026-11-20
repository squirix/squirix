using System;
using System.Collections.Generic;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

[Immutable]
internal sealed class PersistenceOptionsValidator : ISettingsRule<PersistenceOptions>
{
    public IReadOnlyList<string> Check(PersistenceOptions options)
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

        return failures;
    }
}
