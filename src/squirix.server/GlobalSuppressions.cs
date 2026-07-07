using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage(
    "NDepend",
    "ND2500:DontCreateThreadsExplicitly",
    Target = "Squirix.Server.Storage.ManifestRollPublisher..ctor(ManifestStore,Action<Exception>)",
    Justification = "Dedicated manifest roll thread keeps WAL I/O off manifest disk writes; Task.Run is banned on infrastructure paths.")]

[assembly: SuppressMessage(
    "NDepend",
    "ND2500:DontCreateThreadsExplicitly",
    Target = "Squirix.Server.Storage.Journaling.JournalCoordinator..ctor(PersistenceOptions,State,ManifestStore,JournalStartupGate)",
    Justification = "Single-writer journal event loop requires a dedicated long-lived I/O thread; Task.Run is banned on infrastructure paths.")]
