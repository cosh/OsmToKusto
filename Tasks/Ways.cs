using GeoLibrary.IO.GeoJson;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using OsmSharp;
using OsmSharp.Geo;
using OsmSharp.Streams;
using OsmToKusto.Ingestion;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmToKusto.Tasks
{
    public class Ways
    {
        private readonly ILogger _logger;
        private readonly IngestionManager _iManager;
        static private int _ongoingProcessing = 0;
        private ConcurrentQueue<ComplexGeoTask> _workpackages = new ConcurrentQueue<ComplexGeoTask>();

        public Ways(ILoggerFactory loggerFactory, IngestionManager iManager)
        {
            _logger = loggerFactory.CreateLogger<Ways>();
            _iManager = iManager;
        }

        public void IngestAllWays(OSMJob job)
        {
            var command =
            CslCommandGenerator.GenerateTableCreateCommand(
                job.Config.Kusto.RawWaysTable,
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
                    job.Config.Kusto.RawWaysTable,
                    job.Config.Kusto.RawWaysMappingName,
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


            var taskFileName = Path.GetRandomFileName();

            var pbfFileNames = CreateCopies(job.Config.PbfFilePath, job.Config.ComplexGeoTask, job.Config.TempDirectory);

            CreateTasks(job, taskFileName);

            var threads = new List<Task>();

            var tokenSource = new CancellationTokenSource();
            CancellationToken ct = tokenSource.Token;

            TaskFactory tf = new TaskFactory(ct, TaskCreationOptions.LongRunning, TaskContinuationOptions.None, TaskScheduler.Default);

            for (int i = 0; i < job.Config.ComplexGeoTask; i++)
            {
                var tempI = i;
                threads.Add(tf.StartNew(() => StartWorker(pbfFileNames[tempI], ct)));
            }

            while (_workpackages.Count > 0 || _ongoingProcessing > 0)
            {
                _logger.LogInformation($"Complex process queue: {_workpackages.Count}, Ongoing processes : {Volatile.Read(ref _ongoingProcessing)}");

                Thread.Sleep(60000);
            }

            _logger.LogInformation("DONE processing");

            tokenSource.Cancel();

            Task.WaitAll(threads.ToArray());
        }

        private void CreateTasks(OSMJob job, string taskFileName)
        {
            int count = 0;
            using (var fileStream = File.OpenRead(job.Config.PbfFilePath))
            {
                List<Way> ways = new List<Way>();

                // create source stream.
                var source = new PBFOsmStreamSource(fileStream);

                ways = source
                    .Where(_ => _.Type == OsmSharp.OsmGeoType.Way)
                    .Select(__ => (Way)__)
                    .ToList();

                int lastWorkPackage = 0;

                using (StreamWriter taskFileStream = new(taskFileName))
                {
                    foreach (var aWay in ways)
                    {
                        count++;

                        var waysNodes = aWay.Nodes != null ? aWay.Nodes.ToHashSet() : new HashSet<long>();
                        if (aWay.Id.HasValue)
                        {
                            waysNodes.Add(aWay.Id.Value);
                        }

                        if (count % job.Config.ComplexGeoBatchSize == 0)
                        {
                            var complexGeoTask = new ComplexGeoTask() { Skip = count - job.Config.ComplexGeoBatchSize, Take = job.Config.ComplexGeoBatchSize, TaskFileName = taskFileName, osmJob = job };
                            _workpackages.Enqueue(complexGeoTask);

                            lastWorkPackage = count;
                        }

                        taskFileStream.WriteLine(String.Join(Helper.csvSeparator, waysNodes));
                    }
                }

                _workpackages.Enqueue(new ComplexGeoTask() { Skip = lastWorkPackage, Take = count - lastWorkPackage, TaskFileName = taskFileName, osmJob = job });
            }
        }

        private void StartWorker(string pbfFile, CancellationToken ct)
        {
            _logger.LogInformation($"Started a task responsible for file {pbfFile}");

            if (ct.IsCancellationRequested)
            {
                return;
            }

            ComplexGeoTask complexTask;
            while (true)
            {
                while (_workpackages.TryDequeue(out complexTask))
                {
                    if (ct.IsCancellationRequested)
                    {
                        return;
                    }

                    _logger.LogInformation($"Dequeued a task on {pbfFile} (skip: {complexTask.Skip}, take: {complexTask.Take})");

                    Interlocked.Increment(ref _ongoingProcessing);
                    try
                    {
                        ProcessTask(complexTask, pbfFile);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref _ongoingProcessing);
                    }
                }

                Thread.Sleep(10000);
            }
        }

        private List<string> CreateCopies(string pbfFilePath, int complexGeoTask, string tempDirectory)
        {
            List<String> copies = new List<String>();

            for (int i = 0; i < complexGeoTask; i++)
            {
                var taskFileName = Path.GetRandomFileName();
                var fileIncludingPath = Path.Combine(tempDirectory, taskFileName);

                File.Copy(pbfFilePath, fileIncludingPath, true);

                copies.Add(fileIncludingPath);
            }

            return copies;
        }

        private void ProcessTask(ComplexGeoTask complexTask, string pbfFile)
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

                var features = filtered.ToFeatureSource()
                    .Where(_ => _.Geometry is NetTopologySuite.Geometries.LineString && _.Attributes.Exists("id"))
                    .ToDictionary(_ => _.Attributes["id"]);

                var complete = filtered
                    .Where(_ => _.Type == OsmGeoType.Way && _.Id.HasValue)
                    .ToDictionary(_ => _.Id.Value);

                long count = 0;
                long ingestions = 0;
                StringBuilder sb = new StringBuilder();

                foreach (var aWay in complete)
                {
                    NetTopologySuite.Features.IFeature feature;
                    if (features.TryGetValue(aWay.Key, out feature))
                    {
                        var ls = feature.Geometry as NetTopologySuite.Geometries.LineString;
                        var wkt = ls.ToText();
                        try
                        {
                            if (wkt != null && wkt.Contains("LINESTRING"))
                            {
                                var model = GeoLibrary.Model.Geometry.FromWkt(wkt);
                                var geojson = model.ToGeoJson();

                                var _aWay = (Way)aWay.Value;

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

                _logger.LogInformation("DONE ingesting OSM ways");
            }
        }
    }
}
