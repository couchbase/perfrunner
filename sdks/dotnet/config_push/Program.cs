using System.Threading.Tasks;
using Couchbase;
using System;
using Microsoft.Extensions.Logging;
using Couchbase.Core.Diagnostics.Tracing;
using Microsoft.Extensions.DependencyInjection;
using Serilog;
using Serilog.Extensions.Logging;
using Serilog.Sinks.SystemConsole.Themes;

class ConfigPush
{
    Params Pars;
    IBucket? Bucket;

    static readonly CancellationTokenSource TaskTimer = new CancellationTokenSource();

    public static void SetTime(int time)
    {
        TaskTimer.CancelAfter(TimeSpan.FromSeconds(time));
    }

    public ConfigPush(Params pars)
    {
        this.Pars = pars;
    }

    public async Task Connect(){
        try
        {
            // Enable logging
            SetupSerilog();

            var options = new ClusterOptions
            {
                KvTimeout = TimeSpan.FromMilliseconds(this.Pars.Timeout),
                ConfigPollInterval = TimeSpan.FromMilliseconds(this.Pars.ConfigPollInterval)
            };
            options.WithCredentials(username: this.Pars.Username!, password: this.Pars.Password!)
                .WithLogging(new SerilogLoggerFactory());
            var cluster = await Cluster.ConnectAsync("couchbase://" + this.Pars.Host!, options);
            await cluster.WaitUntilReadyAsync(TimeSpan.FromSeconds(30));

            this.Bucket = await cluster.BucketAsync(this.Pars.Bucket!);
        }
        catch (Exception e)
        {
            Serilog.Log.Error("ERROR: {e}", e);
        }
    }

    private void SetupSerilog()
    {
        var outputTemplate = "{Timestamp:HH:mm:ss.fff}, {Message:lj} {Properties:j}{NewLine}{Exception}";
        var loggerConfig = new LoggerConfiguration()
            .Enrich.FromLogContext()
            .MinimumLevel.Information()
            .WriteTo.Console(outputTemplate: outputTemplate, theme: AnsiConsoleTheme.Code)
            // .WriteTo.File("../../config_push_stderr.log")
            ;

        Serilog.Log.Logger = loggerConfig.CreateLogger();
    }

    public async Task Run()
    {

        try
        {
            var scope = await Bucket!.ScopeAsync(this.Pars.Scope!);
            var collection = await scope.CollectionAsync(this.Pars.Coll!);

            while (!TaskTimer.IsCancellationRequested)
            {
                var keys = this.Pars.Keys.ToList();
                var tasks = new List<Task>(keys.Count);
                try
                {
                    foreach (var key in keys)
                    {
                        var doc = NewSampleDoc(key);
                        var t = collection.UpsertAsync(key, doc);
                        tasks.Add(t);
                    }

                    await Task.WhenAll(tasks);
                    Serilog.Log.Information("[SUCCESS] Store all keys");
                }
                catch (CouchbaseException ex)
                {
                    Serilog.Log.Error("[FAILURE] {ex}", ex);
                }
            }
        }
        catch (Exception e)
        {
            Serilog.Log.Error("ERROR: {e}", e);
        }

    }

    SampleDoc NewSampleDoc(string key) => new SampleDoc(
        Id: key,
        Updated: DateTime.UtcNow,
        Field_1: "some_string",
        Field_2: "another_string",
        Field_3: "also a string",
        Field_4: "your best string",
        Field_5: "test_string"
        );
}

class ConfigPushMain
{
    static async Task Main(string[] args)
    {
        var pars = new Params(args);

        var task = new ConfigPush(pars);
        await task.Connect();
        List<Task> tasks = new List<Task>();
        for (int i = 0; i < pars.Threads; ++i)
        {
            tasks.Add(task.Run());
        }
        ConfigPush.SetTime(pars.RunningTime);
        await Task.WhenAll(tasks);
    }
}

class Params
{
    // Cluster
    public string? Host;
    public string? Bucket;
    public string? Username;
    public string? Password;

    // Doc
    public string? Scope = "_default";
    public string? Coll = "_default";

    // Program
    public string[] Keys = { };
    public int Timeout = 2500;
    public int ConfigPollInterval = 2500;
    public int Threads = 1;
    public long Items = 1;
    public int RunningTime = 30;

    public Params(string[] args)
    {
        for (int i = 0; i < args.Length; i += 2)
        {
            string arg = args[i];
            string argValue = (i + 1 < args.Length) ? args[i + 1] : "";
            switch (arg)
            {
                case "--host":
                    this.Host = argValue;
                    break;
                case "--bucket":
                    this.Bucket = argValue;
                    break;
                case "--username":
                    this.Username = argValue;
                    break;
                case "--password":
                    this.Password = argValue;
                    break;
                case "--num-items":
                    this.Items = Int64.Parse(argValue);
                    break;
                case "--num-threads":
                    this.Threads = Int32.Parse(argValue);
                    break;
                case "--timeout":
                    this.Timeout = Int32.Parse(argValue);
                    break;
                case "--keys":
                    this.Keys = argValue.Split(",");
                    break;
                case "--time":
                    this.RunningTime = Int32.Parse(argValue);
                    break;
                case "--c-poll-interval":
                    this.ConfigPollInterval = Int32.Parse(argValue);
                    break;
                default:
                    Console.Error
                            .WriteLine("[WARNING] Unknown option " + arg + ", ignoring value '" + argValue + "'");
                    break;
            }

        }
        Console.Error.WriteLine("Running benchmark for " + this.ToString());
    }

    public override string ToString()
    {
        return this.RunningTime + " sec on host " + this.Host + " (" + this.Bucket
                + ") with [items: " + this.Items + ", threads: " + this.Threads + ", timeout: " + this.Timeout + "ms]";
    }
}

record SampleDoc(string Id,
    DateTime Updated,
    string Field_1,
    string Field_2,
    string Field_3,
    string Field_4,
    string Field_5);
