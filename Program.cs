
using OsmSharp.Streams;
using OsmSharp;
using OsmSharp.Geo;
using NetTopologySuite.Geometries;
using System.Text;
using OsmSharp.Tags;
using Kusto.Ingest;
using Kusto.Data;
using Kusto.Data.Net.Client;
using Kusto.Data.Common;
using System.Text.RegularExpressions;
using Kusto.Data.Ingestion;
using System.Globalization;
using OsmToKusto;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Json;
using Microsoft.Extensions.Logging;
using OsmToKusto.Tasks;
using OsmToKusto.Ingestion;
using System.IO.MemoryMappedFiles;

Settings settings = GetSettings();

var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .AddFilter("Microsoft", LogLevel.Warning)
        .AddFilter("System", LogLevel.Warning)
        .AddFilter("OsmToKusto", LogLevel.Debug)
        .AddSimpleConsole(options =>
        {
            options.IncludeScopes = true;
            options.SingleLine = true;
            options.TimestampFormat = "hh:mm:ss ";
        });

    if (!String.IsNullOrWhiteSpace(settings.APPINSIGHTS_INSTRUMENTATIONKEY))
    {
        builder.AddApplicationInsights(settings.APPINSIGHTS_INSTRUMENTATIONKEY);
    }
});

ILogger logger = loggerFactory.CreateLogger<Program>();

var ingestUri = $"https://ingest-{settings.Kusto.ClusterName}.{settings.Kusto.ClusterRegion}.kusto.windows.net";
var commandAndQueryURL = $"https://{settings.Kusto.ClusterName}.{settings.Kusto.ClusterRegion}.kusto.windows.net";

KustoConnectionStringBuilder ingestConnectionString;
KustoConnectionStringBuilder commandsConnectionString;
if (settings.Kusto.SPAppId == null)
{
    ingestConnectionString = new KustoConnectionStringBuilder(ingestUri).WithAadUserPromptAuthentication(userId: settings.Kusto.UserId);
    commandsConnectionString = new KustoConnectionStringBuilder(commandAndQueryURL).WithAadUserPromptAuthentication(userId: settings.Kusto.UserId);
}
else
{
    ingestConnectionString = new KustoConnectionStringBuilder(ingestUri).WithAadApplicationKeyAuthentication(settings.Kusto.SPAppId, settings.Kusto.SPClientSecret, settings.Kusto.SPTenantId);
    commandsConnectionString = new KustoConnectionStringBuilder(commandAndQueryURL).WithAadApplicationKeyAuthentication(settings.Kusto.SPAppId, settings.Kusto.SPClientSecret, settings.Kusto.SPTenantId);
}

var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestConnectionString);
var kustoClient = KustoClientFactory.CreateCslAdminProvider(commandsConnectionString);

IngestionManager iManager = new IngestionManager(ingestClient, settings, loggerFactory);

var tasks = new List<Task>();

OSMJob job = new OSMJob() { CommandClient = kustoClient, IngestClient = ingestClient, Config = settings};

if (settings.ProcessNodes)
{
    logger.LogInformation($"PROCESSING ALL NODES");
    Task allGeosTask = CreateAllGeosTask(loggerFactory, iManager, job);
    tasks.Add(allGeosTask);
}

if (settings.ProcessWays)
{
    logger.LogInformation($"PROCESSING ALL WAYS");
    Task waysTask = CreateWaysTask(loggerFactory, iManager, job);
    tasks.Add(waysTask);
}

//extracting relations is not ready yet
if (settings.ProcessRelations)
{
    logger.LogInformation($"PROCESSING ALL RELATIONS");
    //Task relationsTask = CreateRelationsTask(loggerFactory, iManager, job);
    //tasks.Add(relationsTask);
}

Task.WaitAll(tasks.ToArray());

while (iManager.GetQueueCount() > 0 || iManager.GetOngoingIngestions() > 0)
{
    logger.LogInformation($"Ingestion queue count: {iManager.GetQueueCount()}, Ongoing ingestions : {iManager.GetOngoingIngestions()}");

    Thread.Sleep(10000);
}

logger.LogInformation("Finished all ingestions, press ENTER to stop the program.");

Console.ReadLine();

Settings GetSettings()
{
    string developmentConfiguration = "appsettingsDevelopment.json";
    string configFile = "appsettings.json";
    string fileUsedForConfiguration = null;

    if (File.Exists(developmentConfiguration))
    {
        fileUsedForConfiguration = developmentConfiguration;
    }
    else
    {
        fileUsedForConfiguration = configFile;
    }

    IConfiguration config = new ConfigurationBuilder()
        .AddJsonFile(fileUsedForConfiguration)
        .AddEnvironmentVariables()
        .Build();

    Settings settings = config.GetRequiredSection("Settings").Get<Settings>();
    return settings;
}

static Task CreateAllGeosTask(ILoggerFactory loggerFactory, IngestionManager iManager, OSMJob job)
{
    AllGeoms allGeoms = new AllGeoms(loggerFactory, iManager);

    Action allGeomsAction = () =>
    {
        allGeoms.IngestAllGeos(job);
    };

    var allGeosTask = Task.Run(allGeomsAction);
    return allGeosTask;
}

static Task CreateWaysTask(ILoggerFactory loggerFactory, IngestionManager iManager, OSMJob job)
{
    IComplexGeo ways = new WaysImpl(loggerFactory, iManager);

    Action waysAction = () =>
    {
        ways.IngestAllComplexGeometries(job);
    };

    var waysTask = Task.Run(waysAction);
    return waysTask;
}

static Task CreateRelationsTask(ILoggerFactory loggerFactory, IngestionManager iManager, OSMJob job)
{
    IComplexGeo ways = new RelationsImpl(loggerFactory, iManager);

    Action waysAction = () =>
    {
        ways.IngestAllComplexGeometries(job);
    };

    var waysTask = Task.Run(waysAction);
    return waysTask;
}
