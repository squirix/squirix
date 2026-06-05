using System;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// journal segment diagnostics for admin storage endpoints.
/// </summary>
public readonly record struct AdminJournalSegmentDiagnosticsSnapshot(
    int Index,
    string Path,
    string FileName,
    bool Exists,
    long LengthBytes,
    DateTime? LastWriteUtc,
    bool HeaderValid,
    string? Error);
