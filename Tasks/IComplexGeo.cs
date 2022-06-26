namespace OsmToKusto.Tasks
{
    public interface IComplexGeo
    {
        void IngestAllComplexGeometries (OSMJob job);
    }
}