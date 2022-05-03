using Couchbase;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Management.Buckets;
using NBomber.Contracts;
using NBomber.CSharp;

IBucket? _bucket = null;
ICouchbaseCollection? _collection = null;
var bucketName = "PerfTesting";
var _cluster = await CreateCluster();


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


//create step to simulate
var step = Step.Create(
    "Couchbase Upsert",
    timeout: TimeSpan.FromMilliseconds(1000),
    execute: async context =>
{
try
{
    if (_cluster == null || _collection == null)
    {
        return Response.Fail();
    }
    else if (_collection != null)
    {
        await PopuplateDatabase(json, _collection);
    }
}
catch (System.Exception ex)
{
    context.Logger.Error($"{ex.Message} - {ex.StackTrace}");
    return Response.Fail();
}
return Response.Ok(sizeBytes: System.Text.ASCIIEncoding.Unicode.GetByteCount(json));
});

//build Scenario to run
var scenario = ScenarioBuilder
    .CreateScenario("Test Couchbase", step)
    .WithLoadSimulations(new[] {
        Simulation.RampConstant(copies: 400, during: TimeSpan.FromSeconds(10)),
        Simulation.KeepConstant(copies: 600, during: TimeSpan.FromSeconds(10)),
        Simulation.InjectPerSec(rate: 600, during: TimeSpan.FromSeconds(10)),
        Simulation.InjectPerSecRandom(minRate: 700, maxRate: 800, during: TimeSpan.FromSeconds(120))
    })
    .WithInit(async context => { await CreateBucket(); });

//run the test
NBomberRunner.RegisterScenarios(scenario)
    .Run();

//clean up resources
_bucket?.Dispose();
_cluster.Dispose();


//work to do
async Task PopuplateDatabase(string json, ICouchbaseCollection collection)
{
    await collection.UpsertAsync($"{System.Guid.NewGuid()}", json);
}


//init cluster
async Task<ICluster> CreateCluster()
{
    //setup database connection
    var cluster = await Cluster.ConnectAsync("couchbase://localhost", options =>
    {
        options.WithCredentials("Administrator", "P@$$w0rd12");
        options.NumKvConnections = 4;
        options.MaxKvConnections = 8;
        options.Experiments.ChannelConnectionPools = false;
        options.WithTracing(new Couchbase.Core.Diagnostics.Tracing.TracingOptions() { Enabled = false });
        options.WithLoggingMeterOptions(new Couchbase.Core.Diagnostics.Metrics.LoggingMeterOptions().Enabled(false));
        options.WithOrphanTracing(new Couchbase.Core.Diagnostics.Tracing.OrphanResponseReporting.OrphanOptions() { Enabled = false });
        options.EnableDnsSrvResolution = false;
        options.Transcoder = new RawJsonTranscoder();
        options.Tuning = new TuningOptions
        {
            MaximumRetainedOperationBuilders = Environment.ProcessorCount * 4  
        };
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
