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

        public bool DryRun { get; set; } = false;

        public SettingsKusto Kusto { get; set; }
    }
}
