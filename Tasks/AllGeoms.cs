﻿using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Microsoft.Extensions.Logging;
using OsmSharp;
using OsmSharp.Streams;
using OsmToKusto.Ingestion;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmToKusto.Tasks
{
    public class AllGeoms
    {
        private readonly ILogger _logger;
        private readonly IngestionManager _iManager;

        public AllGeoms(ILoggerFactory loggerFactory, IngestionManager iManager)
        {
            _logger = loggerFactory.CreateLogger<AllGeoms>();
            _iManager = iManager;
        }

        public void IngestAllGeos(OSMJob job)
        {
            var fileName = Path.GetFileName(job.Config.PbfFilePath);

            var command =
            CslCommandGenerator.GenerateTableCreateCommand(
                job.Config.Kusto.RawOSMTableName,
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

            job.CommandClient.ExecuteControlCommand(job.Config.Kusto.DatabaseName, command);

            command =
                CslCommandGenerator.GenerateTableMappingCreateOrAlterCommand(
                    IngestionMappingKind.Csv,
                    job.Config.Kusto.RawOSMTableName,
                    job.Config.Kusto.RawOSMTableNameMappingName,
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

            job.CommandClient.ExecuteControlCommand(job.Config.Kusto.DatabaseName, command);

            long count = 0;
            int ingestions = 0;
            StringBuilder sb = new StringBuilder();

            using (var fileStream = File.OpenRead(job.Config.PbfFilePath))
            {
                // create source stream.
                var source = new PBFOsmStreamSource(fileStream);

                foreach (var aOSMItem in source)
                {
                    String geoType = aOSMItem.Type.ToString();
                    String ts = aOSMItem.TimeStamp.HasValue ? aOSMItem.TimeStamp.Value.ToString("yyyy-MM-dd HH:mm:ss.fff") : String.Empty;
                    String tags = Helper.CreateTagString(aOSMItem.Tags);
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
                                nodesOrMember = Helper.GetNodesArray(way.Nodes);
                            }
                            break;

                        case OsmGeoType.Relation:
                            var relation = aOSMItem as Relation;
                            if (relation != null)
                            {
                                nodesOrMember = Helper.GetMembersArray(relation.Members);
                            }
                            break;

                        default:
                            break;
                    }

                    if (((count % job.Config.NumberOfRecordsPerFile == 0) && count > 0) || sb.Length > 1000000000)
                    {
                        _logger.LogInformation($"About to ingest {job.Config.NumberOfRecordsPerFile} rows. Current row count: {count}. Ingestion batch: {ingestions}. Queue count: {_iManager.GetQueueCount()}. Concurrent ingestions: {_iManager.GetOngoingIngestions()}");

                        Helper.IngestToKusto(job.Config.Kusto.DatabaseName, job.Config.Kusto.RawOSMTableName, sb,
                            job.Config.Kusto.RawOSMTableNameMappingName, _iManager);

                        ingestions++;

                        sb.Clear();
                    }

                    var newRow = $"{Helper.CleanString(osmId, Helper.csvSeparator)}{Helper.csvSeparator}" +
                    $"{Helper.CleanString(ts, Helper.csvSeparator)}{Helper.csvSeparator}" +
                    $"{Helper.CleanString(tags, Helper.csvSeparator)}{Helper.csvSeparator}" +
                    $"{Helper.CleanString(userId, Helper.csvSeparator)}{Helper.csvSeparator}" +
                    $"{Helper.CleanString(userName, Helper.csvSeparator)}{Helper.csvSeparator}" +
                    $"{Helper.CleanString(version, Helper.csvSeparator)}{Helper.csvSeparator}" +
                    $"{Helper.CleanString(geoType, Helper.csvSeparator)}{Helper.csvSeparator}" +
                    $"{Helper.CleanString(latitude, Helper.csvSeparator)}{Helper.csvSeparator}" +
                    $"{Helper.CleanString(longitude, Helper.csvSeparator)}{Helper.csvSeparator}" +
                    $"{Helper.CleanString(nodesOrMember, Helper.csvSeparator)} {Helper.csvSeparator}" +
                    $"{fileName}";

                    sb.AppendLine(newRow);

                    count++;
                }
            }

            _logger.LogInformation($"About to ingest the last batch. Current row count: {count}");
            Helper.IngestToKusto(job.Config.Kusto.DatabaseName, job.Config.Kusto.RawOSMTableName, sb,
                    job.Config.Kusto.RawOSMTableNameMappingName, _iManager);

            _logger.LogInformation("DONE ingesting OSM geos");
        }

    }
}