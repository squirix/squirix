namespace Squirix.Server.Node.App;

/// <summary>
/// Result of the journal append phase of a durable mutation.
/// </summary>
/// <typeparam name="TResult">Mutation result type.</typeparam>
internal readonly struct DurableMutationPlan<TResult>
{
    private DurableMutationPlan(bool shouldApply, TResult? skipResult)
    {
        ShouldApply = shouldApply;
        SkipResult = skipResult;
    }

    /// <summary>
    /// Gets a value indicating whether the mutation should continue to durability commit and memory apply.
    /// </summary>
    public bool ShouldApply { get; }

    /// <summary>
    /// Gets the result returned when <see cref="ShouldApply" /> is false.
    /// </summary>
    public TResult? SkipResult { get; }

    /// <summary>
    /// Creates a plan that continues to durability commit and memory apply.
    /// </summary>
    /// <returns>An apply plan.</returns>
    public static DurableMutationPlan<TResult> Apply() => new(true, default);

    /// <summary>
    /// Creates a plan that skips durability commit and memory apply.
    /// </summary>
    /// <param name="result">Result to return to the caller.</param>
    /// <returns>A skip plan.</returns>
    public static DurableMutationPlan<TResult> Skip(TResult result) => new(false, result);
}
