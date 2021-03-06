using Kusto.Data.Common;
using Kusto.Ingest;
using System.IO.MemoryMappedFiles;

namespace OsmToKusto
{
    public class OSMJob
    {
        public IKustoQueuedIngestClient IngestClient { get; internal set; }
        public ICslAdminProvider CommandClient { get; internal set; }
        public Settings Config { get; internal set; }
    }
}