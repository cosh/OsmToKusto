using Kusto.Data.Common;
using Kusto.Ingest;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmToKusto.Ingestion
{

    public class IngestionManager
    {
        private readonly ConcurrentQueue<Task> _ingestionTasks = new ConcurrentQueue<Task>();
        private readonly IKustoQueuedIngestClient _ingestClient;
        private readonly Settings _settings;
        private readonly ILogger _logger;
        private int _ongoingIngestions = 0;

        public IngestionManager(IKustoQueuedIngestClient ingestClient, Settings settings, ILoggerFactory loggerFactory)
        {
            _ingestClient = ingestClient;
            _settings = settings;

            _logger = loggerFactory.CreateLogger<IngestionManager>();

            Action action = () =>
            {
                while (true)
                {
                    Task ingestionTask;
                    while (_ingestionTasks.TryDequeue(out ingestionTask))
                    {
                        Interlocked.Increment(ref _ongoingIngestions);
                        ingestionTask.Start();
                        ingestionTask.Wait();
                        Interlocked.Decrement(ref _ongoingIngestions);
                    }

                    Thread.Sleep(1);
                }
            };

            Task.Run(action);
            Task.Run(action);
            Task.Run(action);
            Task.Run(action);
        }

        public int GetOngoingIngestions()
        {
            return _ongoingIngestions;
        }
        
        public void Enqueue(IngestionJob job)
        {
            var task = new Task(Ingest, job);

            _ingestionTasks.Enqueue(task);
        }

        public int GetQueueCount()
        {
            return _ingestionTasks.Count;
        }

        private void Ingest(object jobObj)
        {
            IngestionJob job = (IngestionJob)jobObj;

            if (_settings.DryRun)
            {
                _logger.LogInformation("No ingestion happened because this is configured as a dry run.");
                return;
            }

            int retries = 0;

            while (retries < _settings.Kusto.MaxRetries)
            {
                try
                {
                    IngestionMapping ingestionMapping = new IngestionMapping();
                    ingestionMapping.IngestionMappingReference = job.MappingName;

                    var properties =
                        new KustoQueuedIngestionProperties(job.DatabaseName, job.Table)
                        {
                            Format = DataSourceFormat.psv,
                            IgnoreFirstRecord = false,
                            IngestionMapping = ingestionMapping
                        };

                    _ingestClient.IngestFromStorage(job.ToBeIngested, ingestionProperties: properties);
                    _logger.LogInformation($"Ingested a batch");

                    Thread.Sleep(100);

                    File.Delete(job.ToBeIngested);
                    _logger.LogDebug($"Deleted file {job.ToBeIngested} because of successful ingestion");

                    Thread.Sleep(1000);

                    return;
                }
                catch (Exception e)
                {
                    _logger.LogError($"Error during ingestion (retry {retries}). Message: {e.Message}");
                    retries++;
                    Thread.Sleep(_settings.Kusto.MsBetweenRetries);
                }
            }

            _logger.LogError($"Abandoning the ingestion because of too many retries (retry {retries}).");
            File.Delete(job.ToBeIngested);
        }
    }
}
