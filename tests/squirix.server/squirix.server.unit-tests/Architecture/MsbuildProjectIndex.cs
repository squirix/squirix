using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Xml.XPath;
using Squirix.Attributes;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Indexed view of an MSBuild project document for architecture assertions.</summary>
[Immutable]
internal sealed class MsbuildProjectIndex
{
    private readonly FrozenDictionary<string, List<XPathNavigator>> _includedElements;
    private readonly FrozenDictionary<string, List<string>> _includes;
    private readonly FrozenSet<string> _localNames;
    private readonly FrozenDictionary<string, string> _properties;

    internal MsbuildProjectIndex(
        FrozenDictionary<string, string> properties,
        FrozenDictionary<string, List<string>> includes,
        FrozenDictionary<string, List<XPathNavigator>> includedElements,
        FrozenSet<string> localNames)
    {
        _properties = properties;
        _includes = includes;
        _includedElements = includedElements;
        _localNames = localNames;
    }

    internal bool ContainsElement(string localName) => _localNames.Contains(localName);

    internal List<string>? GetIncludes(string itemName) => _includes.GetValueOrDefault(itemName);

    internal XPathNavigator RequireIncludedElement(string localName, string include)
    {
        Assert.True(_includedElements.TryGetValue(localName, out var elements));

        XPathNavigator? match = null;
        for (var i = 0; i < elements.Count; i++)
        {
            var element = elements[i];
            if (!string.Equals(element.GetAttribute("Include", string.Empty), include, StringComparison.Ordinal))
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
