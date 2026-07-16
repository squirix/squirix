using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Options;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Compaction;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Constructed by the dependency injection container.")]
internal sealed class JournalCompactionOptionsValidator : IValidateOptions<JournalCompactionOptions>
{
    public ValidateOptionsResult Validate(string? name, JournalCompactionOptions options)
    {
        var failures = new List<string>();
        if (options.MinTailSegments < 0)
            failures.Add("journal compaction MinTailSegments cannot be negative.");
        if (options.MinTailBytes < 0)
            failures.Add("journal compaction MinTailBytes cannot be negative.");
        if (options.MinGap < TimeSpan.Zero)
            failures.Add("journal compaction MinGap cannot be negative.");

        return OptionsValidator.ToResult(failures);
    }
}
