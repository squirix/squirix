namespace Squirix.Server.Logging;

/// <summary>Identifies the node component that emits an event.</summary>
/// <typeparam name="T">Component category.</typeparam>
internal interface ILogger<out T> : ILogger
{
    internal static string Name => CategoryName.Of(typeof(T));
}
