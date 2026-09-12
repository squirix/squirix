namespace Squirix.UnitTests;

/// <summary>Custom cache value used to verify options-provided JSON metadata registration.</summary>
internal sealed class CustomCacheValue
{
    /// <summary>Gets or sets the counter value.</summary>
    public int Count { get; set; }

    /// <summary>Gets or sets the display name.</summary>
    public string Name { get; set; } = string.Empty;
}
