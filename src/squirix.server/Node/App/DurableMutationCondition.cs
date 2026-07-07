namespace Squirix.Server.Node.App;

internal readonly record struct DurableMutationCondition<TResult>
{
    private DurableMutationCondition(bool shouldApply, TResult? skipResult)
    {
        ShouldApply = shouldApply;
        SkipResult = skipResult;
    }

    internal bool ShouldApply { get; }

    /// <summary>
    /// Gets the result returned when <see cref="ShouldApply" /> is false.
    /// </summary>
    internal TResult? SkipResult { get; }

    internal static DurableMutationCondition<TResult> Apply() => new(true, default);

    internal static DurableMutationCondition<TResult> Skip(TResult result) => new(false, result);
}
