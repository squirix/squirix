namespace Squirix.E2ETests.Fixtures;

/// <summary>A kind of fault a <see cref="FaultSchedule" /> hands out.</summary>
internal enum FaultKind
{
    /// <summary>A node stops gracefully.</summary>
    Stop = 0,

    /// <summary>A node shuts down without a graceful drain.</summary>
    AbruptStop = 1,

    /// <summary>A stopped node starts again on its data directory.</summary>
    Restart = 2,

    /// <summary>A running node loses every link to its peers.</summary>
    Isolate = 3,

    /// <summary>Every cut link is restored.</summary>
    Heal = 4,
}
