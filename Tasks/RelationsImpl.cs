using GeoLibrary.IO.GeoJson;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Microsoft.Extensions.Logging;
using OsmSharp;
using OsmSharp.Geo;
using OsmSharp.Streams;
using OsmToKusto.Ingestion;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmToKusto.Tasks
{
    public class RelationsImpl : AComplexGeo
    {
        public RelationsImpl(ILoggerFactory loggerFactory, IngestionManager iManager) : base(iManager)
        {
            _logger = loggerFactory.CreateLogger<RelationsImpl>();
        }

        protected override void CreateSchema(OSMJob job)
        {
            var command =
            CslCommandGenerator.GenerateTableCreateCommand(
                job.Config.Kusto.RawRelationsTable,
                new[]
                {
            Tuple.Create("osmId", "System.Int64"),
            Tuple.Create("ts", "System.DateTime"),
            Tuple.Create("osmTags", "System.Object"),
            Tuple.Create("userId", "System.Int64"),
            Tuple.Create("userName", "System.String"),
            Tuple.Create("osmVersion", "System.Int32"),
            Tuple.Create("geojson", "System.Object"),
            Tuple.Create("wkt", "System.String"),
            Tuple.Create("nodesOrMember", "System.Object"),
            Tuple.Create("pbf", "System.String")
                });
            job.CommandClient.ExecuteControlCommand(job.Config.Kusto.DatabaseName, command);

            command =
                CslCommandGenerator.GenerateTableMappingCreateOrAlterCommand(
                    IngestionMappingKind.Csv,
                    job.Config.Kusto.RawRelationsTable,
                    job.Config.Kusto.RawRelationsMappingName,
                    new[] {
            new ColumnMapping() { ColumnName = "osmId", Properties = new Dictionary<string, string>() { { MappingConsts.Ordinal, "0" } } },
            new ColumnMapping() { ColumnName = "ts", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "1" } } },
            new ColumnMapping() { ColumnName = "osmTags", Properties = new Dictionary<string, string>() { { MappingConsts.Ordinal, "2" } } },
            new ColumnMapping() { ColumnName = "userId", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "3" } } },
            new ColumnMapping() { ColumnName = "userName", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "4" } } },
            new ColumnMapping() { ColumnName = "osmVersion", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "5" } } },
            new ColumnMapping() { ColumnName = "geojson", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "6" } } },
            new ColumnMapping() { ColumnName = "wkt", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "7" } } },
            new ColumnMapping() { ColumnName = "nodesOrMember", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "8" } } },
            new ColumnMapping() { ColumnName = "pbf", Properties =  new Dictionary<string, string>() { { MappingConsts.Ordinal, "9" } } }
                });
            job.CommandClient.ExecuteControlCommand(job.Config.Kusto.DatabaseName, command);

            /*

            command = ".create-or-alter function with (folder = \"Update\", skipvalidation = \"true\") Update_RawWays() {" +
                @$"{job.Config.Kusto.RawWaysTable}
                | extend centroid = geo_line_centroid(geojson), length = geo_line_length(geojson), simplified = geo_line_simplify(geojson, 1000)
                | extend longitude = todouble(centroid.coordinates[0]), latitude = todouble(centroid.coordinates[1])
                | extend
                    centroid_h3_low = geo_point_to_h3cell(longitude, latitude, 2), //158 km
                    centroid_h3_mid = geo_point_to_h3cell(longitude, latitude, 5), //8 km
                    centroid_h3_high = geo_point_to_h3cell(longitude, latitude, 9), //174 m
                    centroid_s2_low = geo_point_to_s2cell(longitude, latitude, 6), //108 km	- 156 km
                    centroid_s2_mid = geo_point_to_s2cell(longitude, latitude, 10), //7 km - 10 km
                    centroid_s2_high = geo_point_to_s2cell(longitude, latitude, 16), //106 m - 153 m
                    centroid_geohash_low = geo_point_to_geohash(longitude, latitude, 3), //108 km	- 156 km
                    centroid_geohash_mid = geo_point_to_geohash(longitude, latitude, 5), //7 km - 10 km
                    centroid_geohash_high = geo_point_to_geohash(longitude, latitude, 7) //106 m - 153 m
                | mv-apply point = geojson.coordinates on
                (
                    extend h3 = geo_point_to_h3cell(todouble(point[0]), todouble(point[1]), 9)
                    | summarize covering_h3 = make_set(h3)
                    | mv-expand covering_h3 to typeof(string)
                )" +
                "}";
            job.CommandClient.ExecuteControlCommand(job.Config.Kusto.DatabaseName, command);

            command =
            CslCommandGenerator.GenerateTableCreateCommand(
                job.Config.Kusto.WaysTable,
                new[]
                {
            Tuple.Create("osmId", "System.Int64"),
            Tuple.Create("ts", "System.DateTime"),
            Tuple.Create("osmTags", "System.Object"),
            Tuple.Create("userId", "System.Int64"),
            Tuple.Create("userName", "System.String"),
            Tuple.Create("osmVersion", "System.Int32"),
            Tuple.Create("geojson", "System.Object"),
            Tuple.Create("wkt", "System.String"),
            Tuple.Create("nodesOrMember", "System.Object"),
            Tuple.Create("pbf", "System.String"),
            Tuple.Create("centroid", "System.Object"),
            Tuple.Create("length", "System.Double"),
            Tuple.Create("simplified", "System.Object"),
            Tuple.Create("longitude", "System.Double"),
            Tuple.Create("latitude", "System.Double"),
            Tuple.Create("centroid_h3_low", "System.String"),
            Tuple.Create("centroid_h3_mid", "System.String"),
            Tuple.Create("centroid_h3_high", "System.String"),
            Tuple.Create("centroid_s2_low", "System.String"),
            Tuple.Create("centroid_s2_mid", "System.String"),
            Tuple.Create("centroid_s2_high", "System.String"),
            Tuple.Create("centroid_geohash_low", "System.String"),
            Tuple.Create("centroid_geohash_mid", "System.String"),
            Tuple.Create("centroid_geohash_high", "System.String"),
            Tuple.Create("covering_h3", "System.String")
                });
            job.CommandClient.ExecuteControlCommand(job.Config.Kusto.DatabaseName, command);

            command = $".alter table {job.Config.Kusto.WaysTable} policy update @'[" + "{" + $"\"IsEnabled\": true, \"Source\": \"{job.Config.Kusto.RawWaysTable}\", \"Query\": \"Update_RawWays\", \"IsTransactional\": true, \"PropagateIngestionProperties\": false" + "}]'";
            job.CommandClient.ExecuteControlCommand(job.Config.Kusto.DatabaseName, command);
            */
        }

        protected override void CreateTasks(OSMJob job, string taskFileName)
        {
            CreateTasks_protected<Relation>(job, taskFileName, OsmGeoType.Relation);
        }

        protected override HashSet<long> GetInterestingNodes<T>(T myRelation)
        {
            Relation way = myRelation as Relation;

            return way.Members != null ? way.Members.Select(_ => _.Id).ToHashSet() : new HashSet<long>();
        }

        protected override void ProcessTask(ComplexGeoTask complexTask, string pbfFile)
        {
            var fileName = Path.GetFileName(complexTask.osmJob.Config.PbfFilePath);

            var nodes = File.ReadLines(complexTask.TaskFileName)
                .Skip(complexTask.Skip)
                .Take(complexTask.Take)
                .Select(_ => _.Split(Helper.csvSeparator))
                .SelectMany(__ => __)
                .Select(aString => Int64.Parse(aString))
                .ToHashSet();

            using (var fileStream = File.OpenRead(pbfFile))
            {
                // create source stream.
                var source = new PBFOsmStreamSource(fileStream);

                var filtered = source
                    .Where(_ => nodes.Contains(_.Id.Value)).ToList();

                var linestringNodes = filtered.Where(_ => _.Type == OsmGeoType.Way)
                    .Select(__ => (Way)__)
                    .SelectMany(___ => ___.Nodes)
                    .ToHashSet();

                var additionalNodes = source
                    .Where(_ => linestringNodes.Contains(_.Id.Value)).ToList();

                filtered.AddRange(additionalNodes);

                var features = filtered.ToFeatureSource()
                    .Where(_ => (_.Geometry is NetTopologySuite.Geometries.Polygon || _.Geometry is NetTopologySuite.Geometries.MultiPolygon) && _.Attributes.Exists("id"))
                    .ToDictionary(_ => _.Attributes["id"]);

                var complete = filtered
                    .Where(_ => _.Type == OsmGeoType.Relation && _.Id.HasValue)
                    .ToDictionary(_ => _.Id.Value);
                

                long count = 0;
                long ingestions = 0;
                StringBuilder sb = new StringBuilder();

                foreach (var aRelation in complete)
                {
                    NetTopologySuite.Features.IFeature feature;
                    if (features.TryGetValue(aRelation.Key, out feature))
                    {
                        var ls = feature.Geometry as NetTopologySuite.Geometries.LineString;
                        var wkt = ls.ToText();
                        try
                        {
                            if (wkt != null && wkt.Contains("LINESTRING"))
                            {
                                var model = GeoLibrary.Model.Geometry.FromWkt(wkt);
                                var geojson = model.ToGeoJson();

                                var _aWay = (Way)aRelation.Value;

                                String ts = _aWay.TimeStamp.HasValue ? _aWay.TimeStamp.Value.ToString("yyyy-MM-dd HH:mm:ss.fff") : String.Empty;
                                String tags = Helper.CreateTagString(_aWay.Tags);
                                String userId = _aWay.UserId.HasValue ? _aWay.UserId.Value.ToString() : String.Empty;
                                String userName = _aWay.UserName;
                                String osmId = _aWay.Id.HasValue ? _aWay.Id.Value.ToString() : String.Empty;
                                String version = _aWay.Version.HasValue ? _aWay.Version.Value.ToString() : String.Empty;
                                String visible = _aWay.Visible.HasValue ? _aWay.Visible.Value.ToString() : String.Empty;
                                String nodesOrMember = Helper.GetNodesArray(_aWay.Nodes);

                                if (((count % complexTask.osmJob.Config.NumberOfRecordsPerFile == 0) && count > 0) || sb.Length > 1000000000)
                                {
                                    _logger.LogInformation($"About to ingest {complexTask.osmJob.Config.NumberOfRecordsPerFile} rows. Current row count: {count}. Ingestion batch: {ingestions}. Queue count: {_iManager.GetQueueCount()}. Concurrent ingestions: {_iManager.GetOngoingIngestions()}");

                                    Helper.IngestToKusto(complexTask.osmJob.Config.Kusto.DatabaseName, complexTask.osmJob.Config.Kusto.RawWaysTable, sb,
                                        complexTask.osmJob.Config.Kusto.RawWaysMappingName, _iManager);

                                    ingestions++;

                                    sb.Clear();
                                }

                                var newRow = $"{Helper.CleanString(osmId, Helper.csvSeparator)}{Helper.csvSeparator}" +
                                $"{Helper.CleanString(ts, Helper.csvSeparator)}{Helper.csvSeparator}" +
                                $"{Helper.CleanString(tags, Helper.csvSeparator)}{Helper.csvSeparator}" +
                                $"{Helper.CleanString(userId, Helper.csvSeparator)}{Helper.csvSeparator}" +
                                $"{Helper.CleanString(userName, Helper.csvSeparator)}{Helper.csvSeparator}" +
                                $"{Helper.CleanString(version, Helper.csvSeparator)}{Helper.csvSeparator}" +
                                $"{Helper.CleanString(geojson, Helper.csvSeparator)}{Helper.csvSeparator}" +
                                $"{Helper.CleanString(wkt, Helper.csvSeparator)}{Helper.csvSeparator}" +
                                $"{Helper.CleanString(nodesOrMember, Helper.csvSeparator)} {Helper.csvSeparator}" +
                                $"{fileName}";

                                sb.AppendLine(newRow);

                                count++;
                            }
                        }
                        catch (ArgumentException ae)
                        {
                            //do nothing...
                        }
                        catch (Exception e)
                        {
                            _logger.LogError($"Error parsing WKT from OSM {wkt} ({e.Message})");
                        }
                    }
                }

                _logger.LogInformation($"About to ingest the last batch. Current row count: {count}");
                Helper.IngestToKusto(complexTask.osmJob.Config.Kusto.DatabaseName, complexTask.osmJob.Config.Kusto.RawWaysTable, sb,
                                        complexTask.osmJob.Config.Kusto.RawWaysMappingName, _iManager);

                _logger.LogInformation("DONE ingesting OSM relations");
            }
        }
    }
}
