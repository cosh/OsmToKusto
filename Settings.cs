using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsmToKusto
{
    internal class Settings
    {
        public string APPINSIGHTS_INSTRUMENTATIONKEY { get; set; } = "";

        public string PbfFilePath { get; set; }

        public int NumberOfRecordsPerFile { get; set; } = 750000;

        public SettingsKusto Kusto { get; set; }
    }
}
