using Couchbase;
using Couchbase.Core.IO.Transcoders;
using Couchbase.Extensions.Tracing.Otel.Tracing;
using Couchbase.KeyValue;
using Couchbase.Management.Buckets;
using NBomber.Contracts;
using NBomber.CSharp;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Serilog;
using Serilog.Extensions.Logging;
using System.Net;

IBucket? _bucket = null;
ICouchbaseCollection? _collection = null;
var bucketName = "PerfTesting";
var _cluster = await CreateCluster();
var _clusterCreateMutex = new SemaphoreSlim(1);

/*
 * You can monitor some KPIs with 
 * dotnet-counters.exe monitor --name cb-dotnet-load-test --counters System.Runtime[threadpool-queue-length,threadpool-thread-count,monitor-lock-contention-count],CouchbaseNetClient
 */

// You can try tuning these values, but be aware that it affects everything in the runtime, not just Couchbase.
////ThreadPool.GetMinThreads(out var workerThreads, out var completionPortThreads);
////Console.Out.WriteLine((workerThreads: workerThreads, completionPortThreads: completionPortThreads));
////var successfulThreadpoolResize = ThreadPool.SetMinThreads(Math.Max(workerThreads, 512), Math.Max(completionPortThreads, 256));
////ThreadPool.GetMinThreads(out var workerThreadsUpdated, out var completionPortThreadsUpdated);
////Console.Out.WriteLine((workerThreads: workerThreadsUpdated, completionPortThreads: completionPortThreadsUpdated, updated: successfulThreadpoolResize));

//setup data for insert
var sb = new System.Text.StringBuilder("{\n");
sb.Append("  \"Field_1\": \"****************\",\n");
sb.Append("  \"Field_2\": \"****************\",\n");
sb.Append("  \"Field_3\": \"****************\",\n");
sb.Append("  \"Field_4\": \"****************\",\n");
sb.Append("  \"Field_5\": \"****************\",\n");
sb.Append("  \"Field_6\": \"****************\"\n");
sb.Append("}");
var json = sb.ToString();

// If you continually throw requests in parallel, you will eventually exceed the rate at which operations can actually be dispatched.
// Setting this value lower will protect against SendQueueFull problems under sustained load, but will put an upper limit on the maximum ops/sec.
var maxOpsInFlight = new SemaphoreSlim(120);

//create step to simulate
var step = Step.Create(
    "Couchbase Upsert",
    timeout: TimeSpan.FromMilliseconds(1000),
    execute: async context =>
{
    try
    {
        await maxOpsInFlight.WaitAsync();
        if (_cluster == null || _collection == null)
        {
            return Response.Fail();
        }
        else if (_collection != null)
        {
            await PopuplateDatabase(json, _collection);
        }
    }
    catch (Couchbase.Core.Exceptions.KeyValue.SendQueueFullException ex)
    {
        context.Logger.Error($"SendQueueFull: {ex.Message}");

        // if the send queue is full, back off a bit
        await Task.Delay(100);

        return Response.Fail(ex, statusCode: (int)HttpStatusCode.ServiceUnavailable);
    }
    catch (System.Exception ex)
    {
        context.Logger.Error($"{ex.Message} - {ex.StackTrace}");
        int statusCode = ex is Couchbase.Core.Exceptions.TimeoutException ? (int)HttpStatusCode.GatewayTimeout : (int)HttpStatusCode.InternalServerError;
        return Response.Fail(statusCode: statusCode);
    }
    finally
    {
        maxOpsInFlight.Release();
    }

    return Response.Ok(statusCode: 200, sizeBytes: System.Text.ASCIIEncoding.Unicode.GetByteCount(json));

});

//build Scenario to run
var scenario = ScenarioBuilder
    .CreateScenario("Test Couchbase", step)
    .WithLoadSimulations(new[] {
        Simulation.RampConstant(copies: 300, during: TimeSpan.FromSeconds(10)),
        Simulation.KeepConstant(copies: 900, during: TimeSpan.FromSeconds(20)),
        Simulation.InjectPerSec(rate: 700, during: TimeSpan.FromSeconds(10)),
        Simulation.InjectPerSecRandom(minRate: 500, maxRate: 900, during: TimeSpan.FromSeconds(120))
    })
    .WithInit(async context => { await CreateBucket(); });

//run the test
try
{
    NBomberRunner.RegisterScenarios(scenario)
        .Run();
}
finally
{
    //clean up resources
    _bucket?.Dispose();
    _cluster.Dispose();
    Serilog.Log.CloseAndFlush();
}


//work to do
async Task PopuplateDatabase(string json, ICouchbaseCollection collection)
{
    await collection.UpsertAsync($"{System.Guid.NewGuid()}", json);
}

//init cluster
async Task<ICluster> CreateCluster()
{
    var tracerProvider = Sdk.CreateTracerProviderBuilder()
                .SetSampler(new AlwaysOnSampler())
                .AddJaegerExporter(o =>
                {
                    o.AgentHost = "localhost";
                    o.AgentPort = 6831;
                })
                .AddCouchbaseInstrumentation()
                .Build();

    //setup database connection
    var cluster = await Cluster.ConnectAsync("couchbase://localhost", options =>
    {
        options.WithCredentials("Administrator", "P@$$w0rd12");
        options.NumKvConnections = 8;
        options.MaxKvConnections = 12;

        // this is now th default as of 3.3.0
        options.Experiments.ChannelConnectionPools = true;

        options.WithTracing(new Couchbase.Core.Diagnostics.Tracing.TracingOptions() { Enabled = false });
        options.TracingOptions.WithTracer(new OpenTelemetryRequestTracer());
        options.WithLoggingMeterOptions(new Couchbase.Core.Diagnostics.Metrics.LoggingMeterOptions().Enabled(false));
        options.WithOrphanTracing(new Couchbase.Core.Diagnostics.Tracing.OrphanResponseReporting.OrphanOptions() { Enabled = false });
        options.EnableDnsSrvResolution = false;
        options.Transcoder = new RawJsonTranscoder();
        options.Tuning = new TuningOptions
        {
            MaximumRetainedOperationBuilders = Environment.ProcessorCount * 4  
        };

        // Setting this value higher will allow more operations to be queued.
        // This allows higher parallelism for sustained durations at the expense of more memory use when the queue is being used.
        options.KvSendQueueCapacity = 4096;

        Serilog.Log.Logger = new Serilog.LoggerConfiguration()
        .Enrich.FromLogContext()
        .MinimumLevel.Warning()
        .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss.fff} {Level:u3}] |{Properties:j}| [{SourceContext:l}] {Message:lj}{NewLine}{Exception}")
        .CreateLogger();
        options.WithLogging(new SerilogLoggerFactory());
    });

    return cluster;
}

//init bucket
async Task CreateBucket()
{
    if (_cluster != null)
    {
        await _cluster.Buckets.DropBucketAsync("PerfTesting");
        await Task.Delay(5000);
        var bucketSettings = new BucketSettings
        {
            Name = bucketName, 
            BucketType = BucketType.Couchbase,
            RamQuotaMB = 1024
        };

        await _cluster.Buckets.CreateBucketAsync(bucketSettings);
        await Task.Delay(5000);
        _bucket = await _cluster.BucketAsync(bucketName);
        _collection = await _bucket.DefaultCollectionAsync();
    }
}
