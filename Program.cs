
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

Settings settings = GetSettings();

settings.DryRun = false;

var ways = new List<OsmGeo>();
var relations = new List<OsmGeo>();

var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .AddFilter("Microsoft", LogLevel.Warning)
        .AddFilter("System", LogLevel.Warning)
        .AddFilter("OsmToKusto", LogLevel.Debug)
        .AddConsole();

    if (!String.IsNullOrWhiteSpace(settings.APPINSIGHTS_INSTRUMENTATIONKEY))
    {
        builder.AddApplicationInsights(settings.APPINSIGHTS_INSTRUMENTATIONKEY);
    }
});

ILogger logger = loggerFactory.CreateLogger<Program>();

var fileName = Path.GetFileName(settings.PbfFilePath);

const char csvSeparator = '|';

var ingestUri = $"https://ingest-{settings.Kusto.ClusterName}.{settings.Kusto.ClusterRegion}.kusto.windows.net";
var ingestConnectionStringBuilder = new KustoConnectionStringBuilder(ingestUri).WithAadUserPromptAuthentication(userId: settings.Kusto.UserId);

var commandAndQueryURL = $"https://{settings.Kusto.ClusterName}.{settings.Kusto.ClusterRegion}.kusto.windows.net";
var commandAndQueryConnectionStringBuilder = new KustoConnectionStringBuilder(commandAndQueryURL).WithAadUserPromptAuthentication(userId: settings.Kusto.UserId);

var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestConnectionStringBuilder);
IngestionManager iManager = new IngestionManager(ingestClient, settings, loggerFactory);

using (var kustoClient = KustoClientFactory.CreateCslAdminProvider(commandAndQueryConnectionStringBuilder))
{
    var command =
        CslCommandGenerator.GenerateTableCreateCommand(
            settings.Kusto.RawOSMTableName,
            new[]
            {
                Tuple.Create("osmId", "System.Int64"),
                Tuple.Create("ts", "System.DateTime"),
                Tuple.Create("osmTags", "System.Object"),
                Tuple.Create("userId", "System.Int64"),
                Tuple.Create("userName", "System.String"),
                Tuple.Create("osmVersion", "System.Int32"),
                Tuple.Create("geoType", "System.String"),
                Tuple.Create("latitude", "System.Double"),
                Tuple.Create("longitude", "System.Double"),
                Tuple.Create("nodesOrMember", "System.Object"),
                Tuple.Create("pbf", "System.String")
            });

    kustoClient.ExecuteControlCommand(settings.Kusto.DatabaseName, command);

    command =
        CslCommandGenerator.GenerateTableMappingCreateOrAlterCommand (
            IngestionMappingKind.Csv,
            settings.Kusto.RawOSMTableName,
            settings.Kusto.RawOSMTableNameMappingName,
            new[] {
                new ColumnMapping() { ColumnName = "osmId", Properties = new Dictionary<string, string>() { { MappingConsts.Ordinal, "0" } } },
                new ColumnMapping() { ColumnName = "ts", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "1" } } },
                new ColumnMapping() { ColumnName = "osmTags", Properties = new Dictionary<string, string>() { { MappingConsts.Ordinal, "2" } } },
                new ColumnMapping() { ColumnName = "userId", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "3" } } },
                new ColumnMapping() { ColumnName = "userName", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "4" } } },
                new ColumnMapping() { ColumnName = "osmVersion", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "5" } } },
                new ColumnMapping() { ColumnName = "geoType", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "6" } } },
                new ColumnMapping() { ColumnName = "latitude", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "7" } } },
                new ColumnMapping() { ColumnName = "longitude", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "8" } } },
                new ColumnMapping() { ColumnName = "nodesOrMember", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "9" } } },
                new ColumnMapping() { ColumnName = "pbf", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "10" } } }
        });

    kustoClient.ExecuteControlCommand(settings.Kusto.DatabaseName, command);
}

using (var fileStream = File.OpenRead(settings.PbfFilePath))
{
    // create source stream.
    var source = new PBFOsmStreamSource(fileStream);

    StringBuilder sb = new StringBuilder();

    long count = 0;
    int ingestions = 0;

    foreach (var aOSMItem in source)
    {
        String geoType = aOSMItem.Type.ToString();
        String ts = aOSMItem.TimeStamp.HasValue ? aOSMItem.TimeStamp.Value.ToString("yyyy-MM-dd HH:mm:ss.fff") : String.Empty;
        String tags = CreateTagString(aOSMItem.Tags);
        String userId = aOSMItem.UserId.HasValue ? aOSMItem.UserId.Value.ToString() : String.Empty;
        String userName = aOSMItem.UserName;
        String osmId = aOSMItem.Id.HasValue ? aOSMItem.Id.Value.ToString() : String.Empty;
        String version = aOSMItem.Version.HasValue ? aOSMItem.Version.Value.ToString() : String.Empty;
        String visible = aOSMItem.Visible.HasValue ? aOSMItem.Visible.Value.ToString() : String.Empty;
        String latitude = String.Empty;
        String longitude = String.Empty;
        String nodesOrMember = String.Empty;

        switch (aOSMItem.Type)
        {
            case OsmGeoType.Node:
                var node = aOSMItem as Node;
                if (node != null)
                {
                    latitude = node.Latitude.HasValue ? node.Latitude.Value.ToString("G", CultureInfo.InvariantCulture) : String.Empty; 
                    longitude = node.Longitude.HasValue ? node.Longitude.Value.ToString("G", CultureInfo.InvariantCulture) : String.Empty;
                }
                break;

            case OsmGeoType.Way:
                var way = aOSMItem as Way;
                if (way != null)
                {
                    nodesOrMember = GetNodesArray(way.Nodes);
                }
                break ;

            case OsmGeoType.Relation:
                var relation = aOSMItem as Relation;
                if (relation != null)
                {
                    nodesOrMember = GetMembersArray(relation.Members);
                }
                break;

            default:
                break;
        }

        if (((count % settings.NumberOfRecordsPerFile == 0) && count > 0) || sb.Length > 1000000000)
        {
             logger.LogInformation($"About to ingest {settings.NumberOfRecordsPerFile} rows. Current row count: {count}. Ingestion batch: {ingestions}. Queue count: {iManager.GetQueueCount()}");

            IngestToKusto(ingestConnectionStringBuilder, settings.Kusto.DatabaseName, settings.Kusto.RawOSMTableName, sb, 
                settings.Kusto.RawOSMTableNameMappingName, logger, settings, iManager);

            ingestions++;

            sb.Clear();
        }

        var newRow = $"{CleanString(osmId, csvSeparator)}{csvSeparator}" +
        $"{CleanString(ts, csvSeparator)}{csvSeparator}" +
        $"{CleanString(tags, csvSeparator)}{csvSeparator}" +
        $"{CleanString(userId, csvSeparator)}{csvSeparator}" +
        $"{CleanString(userName, csvSeparator)}{csvSeparator}" +
        $"{CleanString(version, csvSeparator)}{csvSeparator}" +
        $"{CleanString(geoType, csvSeparator)}{csvSeparator}" +
        $"{CleanString(latitude, csvSeparator)}{csvSeparator}" +
        $"{CleanString(longitude, csvSeparator)}{csvSeparator}" +
        $"{CleanString(nodesOrMember, csvSeparator)} {csvSeparator}" +
        $"{fileName}";

        sb.AppendLine(newRow);

        count++;
    }

    logger.LogInformation($"About to ingest the last batch. Current row count: {count}");
    IngestToKusto(ingestConnectionStringBuilder, settings.Kusto.DatabaseName, settings.Kusto.RawOSMTableName, sb, 
        settings.Kusto.RawOSMTableNameMappingName, logger, settings, iManager);

    logger.LogInformation("DONE ingesting OSM geos");

    logger.LogInformation("Creating features for ways");
    var features = ways.ToFeatureSource();

    while(iManager.GetQueueCount() > 0)
    {
        logger.LogInformation($"Ingestion queue count: {iManager.GetQueueCount()}");

        Thread.Sleep(10000);
    }

    logger.LogInformation("Finished all ingestions");

    Console.ReadLine();
}

string CleanString(string osmId, char csvSeparater)
{
    return Regex.Replace(osmId.Replace(csvSeparater, '#'), @"\r\n?|\n", "");
}

string GetMembersArray(RelationMember[] members)
{
    if (members != null && members.Length > 0)
    {
        return "[" + String.Join(',', members.Select(_ => $"[\"{_.Id}\", \"{_.Role}\", \"{_.Type}\"]")) + "]";
    }

    return String.Empty;
}

string GetNodesArray(long[] nodes)
{
    if (nodes != null && nodes.Length > 0)
    {
        return "[" + String.Join(',', nodes.Select(_ => $"[\"{_}\", \"\", \"Node\"]")) + "]";
    }

    return String.Empty;
}

string CreateTagString(TagsCollectionBase tags)
{
    if(tags != null && tags.Count > 0)
    {
        return "[" + String.Join(',', tags.Select(aTag => $"[\"{aTag.Key}\", \"{aTag.Value}\"]")) + "]";
    }

    return String.Empty;
}

static void IngestToKusto(KustoConnectionStringBuilder ingestConnectionStringBuilder, string databaseName, string table, 
    StringBuilder sb, String mappingName, ILogger logger, Settings settings, IngestionManager iManager)
{
    var job = new IngestionJob();
    job.Table = table;
    job.MappingName = mappingName;
    job.DatabaseName = databaseName;

    var myByteArray = System.Text.Encoding.UTF8.GetBytes(sb.ToString());
    job.Stream = new MemoryStream(myByteArray);

    iManager.Enqueue(job);
}

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