namespace Squirix.Server.Node.App;

internal readonly record struct DurableMutationCondition<TResult>
{
    private DurableMutationCondition(bool shouldApply, TResult? skipResult)
    {
        ShouldApply = shouldApply;
        SkipResult = skipResult;
    }

    public bool ShouldApply { get; }

    /// <summary>
    /// Gets the result returned when <see cref="ShouldApply" /> is false.
    /// </summary>
    public TResult? SkipResult { get; }

    public static DurableMutationCondition<TResult> Apply() => new(true, default);

    public static DurableMutationCondition<TResult> Skip(TResult result) => new(false, result);
}
