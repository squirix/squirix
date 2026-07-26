using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Xml.Linq;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Indexed view of an MSBuild project document for architecture assertions.</summary>
internal sealed class MsbuildProjectIndex
{
    private readonly FrozenDictionary<string, List<XElement>> _includedElements;
    private readonly FrozenDictionary<string, List<string>> _includes;
    private readonly FrozenSet<string> _localNames;
    private readonly FrozenDictionary<string, string> _properties;

    internal MsbuildProjectIndex(
        FrozenDictionary<string, string> properties,
        FrozenDictionary<string, List<string>> includes,
        FrozenDictionary<string, List<XElement>> includedElements,
        FrozenSet<string> localNames)
    {
        _properties = properties;
        _includes = includes;
        _includedElements = includedElements;
        _localNames = localNames;
    }

    internal bool ContainsElement(string localName) => _localNames.Contains(localName);

    internal List<string> GetIncludes(string itemName) => _includes.TryGetValue(itemName, out var list) ? list : [];

    internal XElement RequireIncludedElement(string localName, string include)
    {
        Assert.True(_includedElements.TryGetValue(localName, out var elements));

        XElement? match = null;
        for (var i = 0; i < elements.Count; i++)
        {
            var element = elements[i];
            if (!string.Equals(element.Attribute("Include")?.Value, include, StringComparison.Ordinal))
                continue;
            match = element;
            break;
        }

        Assert.True(match is not null);
        return match;
    }

    internal string RequireProperty(string propertyName)
    {
        Assert.True(_properties.TryGetValue(propertyName, out var value));
        return value;
    }
}
