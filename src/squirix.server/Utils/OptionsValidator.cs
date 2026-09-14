using System.Collections.Generic;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Utils;

internal static class OptionsValidator
{
    internal static ValidateOptionsResult ToResult(List<string> failures) => failures.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(failures);
}
