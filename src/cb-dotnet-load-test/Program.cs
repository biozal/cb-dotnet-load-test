using Couchbase;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;
using Couchbase.Management.Buckets;
using NBomber.Contracts;
using NBomber.CSharp;

IBucket? _bucket = null;
ICouchbaseCollection? _collection = null;
var bucketName = "PerfTesting";

//setup database connection
var cluster = await Cluster.ConnectAsync("couchbase://localhost", options =>
{
    options.WithCredentials("Administrator", "password");
    options.NumKvConnections = 4;
    options.MaxKvConnections = 4;
    options.Experiments.ChannelConnectionPools = false;
    options.WithTracing(new Couchbase.Core.Diagnostics.Tracing.TracingOptions() { Enabled = false });
    options.WithLoggingMeterOptions(new Couchbase.Core.Diagnostics.Metrics.LoggingMeterOptions().Enabled(false));
    options.WithOrphanTracing(new Couchbase.Core.Diagnostics.Tracing.OrphanResponseReporting.OrphanOptions() { Enabled = false });
    options.EnableDnsSrvResolution = false;
    options.Transcoder = new RawJsonTranscoder();
    options.Tuning = new TuningOptions
    {
        MaximumRetainedOperationBuilders = Environment.ProcessorCount * 16
    };
});

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
        if (_collection != null)
        {
            await PopuplateDatabase(json, _collection);
        }
    } 
    catch (System.Exception ex) 
    {
        context.Logger.Error($"{ex.Message} - {ex.StackTrace}");
        return Response.Fail();
    }
    return Response.Ok();
});

//build Scenario to run
var scenario = ScenarioBuilder
    .CreateScenario("Test Couchbase", step)
    .WithLoadSimulations(new[] {
        Simulation.RampConstant(copies: 1, during: TimeSpan.FromSeconds(5)),
        Simulation.KeepConstant(copies: 10000, during: TimeSpan.FromSeconds(60)),
        Simulation.InjectPerSec(rate: 10, during: TimeSpan.FromSeconds(30)),
        Simulation.InjectPerSecRandom(minRate: 100, maxRate: 200, during: TimeSpan.FromSeconds(10))
    })
    .WithInit(async context => { await CreateBucket(); });

//run the test
NBomberRunner.RegisterScenarios(scenario)
    .Run();

//clean up resources
_bucket?.Dispose();
cluster.Dispose();

//work to do
async Task PopuplateDatabase(string json, ICouchbaseCollection collection)
{
    await collection.UpsertAsync($"{System.Guid.NewGuid()}", json);
}

//init bucket
async Task CreateBucket()
{
    if (cluster != null)
    {
        await cluster.Buckets.DropBucketAsync("PerfTesting");
        await Task.Delay(5000);
        var bucketSettings = new BucketSettings
        {
            Name = bucketName, 
            BucketType = BucketType.Couchbase,
            RamQuotaMB = 1024
        };

        await cluster.Buckets.CreateBucketAsync(bucketSettings);
        await Task.Delay(5000);
        _bucket = await cluster.BucketAsync(bucketName);
        _collection = await _bucket.DefaultCollectionAsync();
    }
}
