using Microsoft.Extensions.Logging;
using OsmSharp;
using OsmSharp.Streams;
using OsmToKusto.Ingestion;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmToKusto.Tasks
{
    public abstract class AComplexGeo : IComplexGeo
    {
        protected ILogger _logger;
        protected readonly IngestionManager _iManager;
        protected int _ongoingProcessing = 0;
        protected ConcurrentQueue<ComplexGeoTask> _workpackages = new ConcurrentQueue<ComplexGeoTask>();

        protected AComplexGeo(IngestionManager iManager)
        {
            _iManager = iManager;
        }
        public void IngestAllComplexGeometries(OSMJob job)
        {
            CreateSchema(job);

            var pbfFileNames = CreateCopies(job.Config.PbfFilePath, job.Config.ComplexGeoTask, job.Config.TempDirectory);

            var taskFileName = Path.GetRandomFileName();

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
        }

        protected abstract void CreateTasks(OSMJob job, string taskFileName);

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

        protected abstract void CreateSchema(OSMJob job);

        protected void CreateTasks_protected<T>(OSMJob job, string taskFileName, OsmSharp.OsmGeoType geoType) where T: OsmGeo
        {
            int count = 0;
            using (var fileStream = File.OpenRead(job.Config.PbfFilePath))
            {
                List<T> compexGeos = new List<T>();

                // create source stream.
                var source = new PBFOsmStreamSource(fileStream);

                compexGeos = source
                    .Where(_ => _.Type == geoType)
                    .Select(__ => (T)__)
                    .ToList();

                int lastWorkPackage = 0;

                using (StreamWriter taskFileStream = new(taskFileName))
                {
                    foreach (var aComplexGeo in compexGeos)
                    {
                        count++;

                        HashSet<long>  interestingNodes = GetInterestingNodes(aComplexGeo);

                        if (aComplexGeo.Id.HasValue)
                        {
                            interestingNodes.Add(aComplexGeo.Id.Value);
                        }

                        if (count % job.Config.ComplexGeoBatchSize == 0)
                        {
                            var complexGeoTask = new ComplexGeoTask() { Skip = count - job.Config.ComplexGeoBatchSize, Take = job.Config.ComplexGeoBatchSize, TaskFileName = taskFileName, osmJob = job };
                            _workpackages.Enqueue(complexGeoTask);

                            lastWorkPackage = count;
                        }

                        taskFileStream.WriteLine(String.Join(Helper.csvSeparator, interestingNodes));
                    }
                }

                _workpackages.Enqueue(new ComplexGeoTask() { Skip = lastWorkPackage, Take = count - lastWorkPackage, TaskFileName = taskFileName, osmJob = job });
            }
        }

        protected abstract HashSet<long> GetInterestingNodes<T>(T aWay) where T : OsmGeo;

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

        protected abstract void ProcessTask(ComplexGeoTask complexTask, string pbfFile);
    }
}
