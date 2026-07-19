using System;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Active trace scope for a journal operation.</summary>
internal interface IJournalOperationTraceScope : IDisposable;
