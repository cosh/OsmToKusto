namespace OsmToKusto
{
    public class IngestionJob
    {
        public string MappingName { get; internal set; }
        public string DatabaseName { get; internal set; }
        public string Table { get; internal set; }
        public MemoryStream Stream { get; internal set; }
    }
}