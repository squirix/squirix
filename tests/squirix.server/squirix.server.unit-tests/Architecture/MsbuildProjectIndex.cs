using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Xml.XPath;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

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

    internal async Task<XPathNavigator> RequireIncludedElement(string localName, string include)
    {
        _ = await Assert.That(_includedElements.TryGetValue(localName, out var elements)).IsTrue();
        var present = (await Assert.That(elements).IsNotNull())!;

        XPathNavigator? match = null;
        for (var i = 0; i < present.Count; i++)
        {
            var element = present[i];
            if (!string.Equals(element.GetAttribute("Include", string.Empty), include, StringComparison.Ordinal))
                continue;
            match = element;
            break;
        }

        return await Assert.That(match).IsNotNull();
    }

    internal async Task<string> RequireProperty(string propertyName)
    {
        _ = await Assert.That(_properties.TryGetValue(propertyName, out var value)).IsTrue();
        return await Assert.That(value).IsNotNull();
    }
}
