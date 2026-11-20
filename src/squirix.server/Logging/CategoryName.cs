using System;

namespace Squirix.Server.Logging;

/// <summary>Builds the stable diagnostic category name for a component type.</summary>
internal static class CategoryName
{
    internal static string Of(Type component)
    {
        var name = component.Name;
        var arity = name.IndexOf('`', StringComparison.Ordinal);
        if (arity >= 0)
            name = name[..arity];

        var prefix = component.DeclaringType is { } parent ? Of(parent) : component.Namespace;
        return prefix == null ? name : $"{prefix}.{name}";
    }
}
