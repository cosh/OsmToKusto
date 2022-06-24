using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmToKusto.Tasks
{
    public class ComplexGeoTask
    {
        public int Skip { get; set; }
        public int Take { get; set; }
        public string TaskFileName { get; set; }

        public OSMJob osmJob { get; set; }
    }
}
