using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmToKusto
{
    public class SettingsKusto
    {
        public string UserId { get; set; }
        public string ClusterName { get; set; }

        public string ClusterRegion { get; set; }

        public string DatabaseName { get; set; } = "osm";

        public string RawAllGeometriesTable { get; set; } = "rawAllGeometries";

        public string RawGeometriesMappingName { get; set; } = "map";

        public int MaxRetries { get; set; } = 10;
        public int MsBetweenRetries { get; set; } = 300000;
        public string RawWaysTable { get; set; } = "rawWays";
        public string RawWaysMappingName { get; set; } = "map";
        public string WaysTable { get; set; } = "ways";
        public string AllGeometriesTable { get; set; } = "allGeometries";




    }
}
