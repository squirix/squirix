using System;

namespace Squirix.Server.Composition;

internal interface INodeRequestScopes
{
    bool Contains(Type type);

    NodeRequestScope Open();
}
