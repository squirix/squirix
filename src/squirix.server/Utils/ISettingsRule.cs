using System.Collections.Generic;

namespace Squirix.Server.Utils;

internal interface ISettingsRule<in T>
{
    IReadOnlyList<string> Check(T options);
}
