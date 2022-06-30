using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmToKusto
{
    public class Settings
    {
        public string APPINSIGHTS_INSTRUMENTATIONKEY { get; set; } = "";

        public string PbfFilePath { get; set; }

        public int NumberOfRecordsPerFile { get; set; } = 750000;

        public int ComplexGeoBatchSize { get; set; } = 130000;

        public int ComplexGeoTask { get; set; } = 10;

        public string TempDirectory { get; set; }

        public bool DryRun { get; set; } = false;

        public bool ProcessWays { get; set; }

        public bool ProcessRelations { get; set; }

        public bool ProcessNodes { get; set; }

        public SettingsKusto Kusto { get; set; }
    }
}
