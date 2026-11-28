using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>A validated key borrowed from the current request and its namespaced lookup hash.</summary>
/// <param name="Bytes">The UTF-8 bytes, valid for the duration of the request.</param>
/// <param name="Hash">The hash shared with native text lookups in the same namespace.</param>
[Immutable]
internal readonly record struct Utf8CacheKey(ReadOnlyMemory<byte> Bytes, int Hash);
