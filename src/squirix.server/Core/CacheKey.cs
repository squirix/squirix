namespace Squirix.Server.Core;

internal sealed record CacheKey(string Namespace, string Key)
{
    public override string ToString() => string.IsNullOrEmpty(Namespace) ? Key : Namespace + ":" + Key;

    internal static CacheKey Default(string key) => new(ServerCacheNames.DefaultNamespace, key);
}
