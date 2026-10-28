using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Transport.Grpc;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Unit tests for <see cref="RpcOperationIdentity" />.</summary>
[Immutable]
public sealed class RpcOperationIdentityTests
{
    private const int ThreadCount = 8;
    private const int IdsPerThread = 12_500;
    private const int IdsAcrossRefills = 2_000;

    /// <summary>Ensures identifiers are 32 lowercase hex characters.</summary>
    [Test]
    public async Task NewReturnsLowercaseHex()
    {
        for (var i = 0; i < IdsAcrossRefills; i++)
        {
            var id = RpcOperationIdentity.New();

            _ = await Assert.That(id.Length).IsEqualTo(32);
            _ = await Assert.That(IsLowercaseHex(id)).IsTrue();
        }
    }

    /// <summary>Ensures identifiers on one thread stay distinct across several random buffer refills.</summary>
    [Test]
    public async Task NewIsUniqueAcrossBufferRefills()
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < IdsAcrossRefills; i++)
            _ = seen.Add(RpcOperationIdentity.New());

        _ = await Assert.That(seen.Count).IsEqualTo(IdsAcrossRefills);
    }

    /// <summary>Ensures identifiers generated concurrently on several threads are all distinct.</summary>
    [Test]
    public async Task NewIsUniqueAcrossThreads()
    {
        var tasks = new Task<string[]>[ThreadCount];
        for (var t = 0; t < ThreadCount; t++)
            tasks[t] = Task.Factory.StartNew(Generate, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        var results = await Task.WhenAll(tasks);

        var seen = new HashSet<string>(ThreadCount * IdsPerThread, StringComparer.Ordinal);
        foreach (var ids in results)
            AddAll(seen, ids);

        _ = await Assert.That(seen.Count).IsEqualTo(ThreadCount * IdsPerThread);
    }

    private static void AddAll(HashSet<string> seen, string[] ids)
    {
        foreach (var id in ids)
            _ = seen.Add(id);
    }

    private static string[] Generate()
    {
        var ids = new string[IdsPerThread];
        for (var i = 0; i < ids.Length; i++)
            ids[i] = RpcOperationIdentity.New();

        return ids;
    }

    private static bool IsLowercaseHex(string value)
    {
        foreach (var c in value)
        {
            if (c is not ((>= '0' and <= '9') or (>= 'a' and <= 'f')))
                return false;
        }

        return true;
    }
}
