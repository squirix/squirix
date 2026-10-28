using System.Text;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of an outbound inter-node unary call through the client interceptor when no trace listener is attached and the call has no deadline.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class ClientInterceptorBenchmarks
{
    private const string Request = "req";
    private const string Response = "ok";
    private static readonly Marshaller<string> StringMarshaller = Marshallers.Create(static value => Encoding.UTF8.GetBytes(value), static bytes => Encoding.UTF8.GetString(bytes));
    private static readonly Method<string, string> UnaryMethod = new(MethodType.Unary, "Bench", "Echo", StringMarshaller, StringMarshaller);
    private static readonly Task<string> CompletedResponse = Task.FromResult(Response);
    private static readonly Task<Metadata> CompletedHeaders = Task.FromResult(Metadata.Empty);
    private readonly ClientInterceptor _interceptor = new();
    private readonly ClientInterceptorContext<string, string> _context = new(UnaryMethod, "localhost", default);

    /// <summary>Sends one unary call through the interceptor over a completed inner call and awaits the response.</summary>
    /// <returns>The response.</returns>
    [Benchmark]
    public async Task<string> UnaryCallAsync()
    {
        using var call = _interceptor.AsyncUnaryCall(Request, _context, static (_, _) => CreateCompletedCallAsync());
        return await call.ResponseAsync.ConfigureAwait(false);
    }

    private static AsyncUnaryCall<string> CreateCompletedCallAsync() => new(CompletedResponse, CompletedHeaders, static () => Status.DefaultSuccess, static () => Metadata.Empty, static () => { });
}
